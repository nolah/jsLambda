package com.jslambda.manager

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.{Member, MemberStatus}
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import com.jslambda.coordinator.CoordinatorActor.{AdjustClusterSize, ExecutionersRemoved}
import com.jslambda.manager.SubClusterManager._
import com.jslambda.manager.NodeProvider.{StartCoordinator, StartExecutioner}
import com.jslambda.manager.SuperClusterManager.{CoordinatorJoined, CoordinatorRecognized, ExecutionerJoined, ExecutionerRecognized}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.jslambda.manager.SubClusterManager.NodeType.NodeType

import scala.concurrent.duration.FiniteDuration
import scala.collection.mutable.Map

object SubClusterManager {
  def props(uuid: String, script: String, startingTcpPort: Int, httpPort: Int, minExecutors: Int, nodeProvider: ActorRef) = Props(new SubClusterManager(uuid, script, startingTcpPort, httpPort, minExecutors, nodeProvider))

  case class AddNewExecutioners(additions: Int, uuid: String)

  case class RemoveExecs(execs: List[ActorRef])

  case class ExecutionerShutdown(uuid: String)

  case class NodeDetails(var member: Member, var nodeType: NodeType, var ref: Option[ActorRef])

  object NodeState extends Enumeration {
    type NodeState = Value
    val Up, Down, ScheduledForShutdown = Value
  }

  object NodeType extends Enumeration {
    type NodeType = Value
    val Executioner, Coordinator = Value
  }

}

class SubClusterManager(uuid: String, script: String, startingTcpPort: Int, httpPort: Int, minExecutors: Int, nodeProvider: ActorRef) extends Actor with ActorLogging {

  private val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe(uuid + "-bus", self)

  implicit val ec = context.dispatcher
  implicit val timeout: Timeout = FiniteDuration(10, java.util.concurrent.TimeUnit.SECONDS)


  var coordinatorNode: Option[NodeDetails] = None
  var executionerNodes: Map[Int, NodeDetails] = Map()

  var port = startingTcpPort

  log.info("Starting sub cluster for uuid: {}", uuid)
  port += 1
  nodeProvider ! StartCoordinator(uuid, port, httpPort)
  (0 until minExecutors) foreach (i => {
    port += 1
    nodeProvider ! StartExecutioner(uuid, port)
  })

  override def receive: Receive = {
    case message: CoordinatorJoined =>
      log.info("CoordinatorJoined: {}", message)
      coordinatorNode match {
        case Some(coordinator) =>
          coordinator.ref = Some(message.actorRef)
          message.actorRef ! CoordinatorRecognized(uuid, executionerNodes.values.flatMap(_.ref).toList)

        case None =>
          log.error("No coordinator node found for actor trying to join subcluster: {}", message.actorRef)
      }


    case message: ExecutionerJoined =>
      log.info("ExecutionerJoined: {}", message)
      message.actorRef.path.address.port match {
        case Some(actorPort) =>
          executionerNodes.get(actorPort) match {
            case Some(executionerNode) =>
              executionerNode.ref = Some(message.actorRef)
              message.actorRef ! ExecutionerRecognized(uuid)
            case None =>
              log.error("No executioner found for given port")
          }


        case None =>
          log.error("Cannot extract port from actor with address: {}", message.actorRef.path.address)
      }

    case message: AdjustClusterSize =>
      log.info("AdjustClusterSize: {}", message)
      if (executionerNodes.size < message.preferredSize) {
        log.info("Adding executioner for uuid: {}", message.uuid)
        (0 until (message.preferredSize - executionerNodes.size)) foreach (i => {
          port += 1
          nodeProvider ! StartExecutioner(uuid, port)
        })
      } else if (executionerNodes.size > message.preferredSize && message.preferredSize > minExecutors) {
        log.info("Removing executioners for uuid: {}", message.uuid)
        val toShutDown = Math.min(executionerNodes.size - message.preferredSize, executionerNodes.size - minExecutors)
        val execNodes = executionerNodes.take(toShutDown).toList
        executionerNodes --= execNodes.map(_._1)

        coordinatorNode match {
          case Some(node) =>
            node.ref match {
              case Some(coordinatorRef) =>
                val execRefs: List[ActorRef] = execNodes.flatMap(_._2.ref)
                pipe(coordinatorRef ? RemoveExecs(execRefs)) to self
              case None =>
                log.error("No coordinator ref found!")
            }
          case None =>
            log.error("No coordinator node active!")
        }
      }

    case message: ExecutionersRemoved =>
      log.info("ExecutionersRemoved: {}", message)
      message.execs.foreach(_ ! ExecutionerShutdown(uuid))

    case s: String =>
      log.info("Got {}", s)
    case SubscribeAck(Subscribe(_, None, `self`)) =>
      log.info("subscribing")
    case MemberJoined(member) =>
      log.info("MemberJoined: {}.", member)
    case MemberUp(member) =>
      log.info("MemberUp: {}.", member)
      member.address.port match {
        case Some(memberPort) =>
          nodeTypeByPort(startingTcpPort, memberPort) match {
            case NodeType.Coordinator =>
              log.info("Coordinator node joined subcluster")
              coordinatorNode = Some(NodeDetails(member, NodeType.Coordinator, None))
            case NodeType.Executioner =>
              log.info("Executioner node joined subcluster")
              executionerNodes += (memberPort -> NodeDetails(member, NodeType.Executioner, None))
          }
        case None =>
          log.error("Member without the port joined cluster: {}", member)
      }
    case MemberWeaklyUp(member) =>
      log.info("MemberWeaklyUp: {}.", member)
    case MemberExited(member) =>
      log.info("MemberExited: {}.", member)
    case MemberRemoved(member, previousState) =>
      if (previousState == MemberStatus.Exiting) {
        log.info("Member {} Previously gracefully exited, REMOVED.", member)
      } else {
        log.info("Member {} Previously downed after unreachable, REMOVED.", member)
      }
      member.address.port match {
        case Some(memberPort) =>
          nodeTypeByPort(startingTcpPort, memberPort) match {
            case NodeType.Coordinator =>
              log.info("Coordinator node removed from cluster, starting new one!")
              nodeProvider ! StartCoordinator(uuid, startingTcpPort + 1, httpPort)
            case NodeType.Executioner =>
              executionerNodes.get(memberPort) match {
                case Some(_) =>
                  log.info("Executioner node on port: {} failed, starting new one!", memberPort)
                  port += 1
                  nodeProvider ! StartExecutioner(uuid, port)
                case None =>
                  log.info("Executioner node on port: {} is removed after being scheduled for shutdown")
              }
          }
        case None =>
      }
    case UnreachableMember(member) =>
      log.info("UnreachableMember: {}.", member)
    case ReachableMember(member) =>
      log.info("ReachableMember: {}.", member)

  }


  def nodeTypeByPort(startingPort: Int, port: Int) = if (startingPort + 1 == port) NodeType.Coordinator else NodeType.Executioner

}
