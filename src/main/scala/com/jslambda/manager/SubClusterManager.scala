package com.jslambda.manager

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.{Member, MemberStatus}
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import com.jslambda.coordinator.CoordinatorActor.{CoordinatorStats, KeepExecutioners}
import com.jslambda.manager.SubClusterManager._
import com.jslambda.manager.NodeProvider.{StartCoordinatorNode, StartExecutionerNode}
import com.jslambda.manager.SuperClusterManager.{CoordinatorJoined, CoordinatorRecognized, ExecutionerJoined, ExecutionerRecognized}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.jslambda.coordinator.CoordinatorActor
import com.jslambda.manager.SubClusterManager.NodeType.NodeType

import scala.concurrent.duration.FiniteDuration
import scala.collection.mutable.Map

object SubClusterManager {
  def props(uuid: String, script: String, startingTcpPort: Int, httpPort: Int, minExecutors: Int, nodeProvider: ActorRef) = Props(new SubClusterManager(uuid, script, startingTcpPort, httpPort, minExecutors, nodeProvider))

  case class AddNewExecutioners(additions: Int, uuid: String)

  case class UseTheseExecutioners(execs: List[ActorRef])

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

  log.info("SUBCLUSTER: {}| Starting", uuid)
  port += 1
  nodeProvider ! StartCoordinatorNode(uuid, port, httpPort)
  (0 until minExecutors) foreach (i => {
    port += 1
    nodeProvider ! StartExecutionerNode(uuid, port)
  })

  override def receive: Receive = {
    case message: CoordinatorJoined =>
      coordinatorNode match {
        case Some(coordinator) =>
          log.info("SUBCLUSTER: {}| CoordinatorJoined: {} success", uuid, message)
          coordinator.ref = Some(message.actorRef)
          message.actorRef ! CoordinatorRecognized(uuid, executionerNodes.values.flatMap(_.ref).toList)

        case None =>
          log.error("SUBCLUSTER: {}| CoordinatorJoined: {} fail", uuid, message.actorRef)
      }


    case message: ExecutionerJoined =>
      message.actorRef.path.address.port match {
        case Some(actorPort) =>
          executionerNodes.get(actorPort) match {
            case Some(executionerNode) =>
              log.info("SUBCLUSTER: {}| ExecutionerJoined: {} success", uuid, message)
              executionerNode.ref = Some(message.actorRef)
              message.actorRef ! ExecutionerRecognized(uuid)
              coordinatorNode.foreach(_.ref.foreach(_ ! UseTheseExecutioners(executionerNodes.flatMap(_._2.ref).toList)))
            case None =>
              log.error("SUBCLUSTER: {}| ExecutionerJoined: {} fail can't find node", uuid, message)
            //              message.actorRef ! ExecutionerShutdown(uuid)
          }


        case None =>
          log.error("SUBCLUSTER: {}| ExecutionerJoined: {} fail can't extract port", uuid, message.actorRef.path.address)
        //          message.actorRef ! ExecutionerShutdown(uuid)
      }

    case message: CoordinatorStats =>
      val preferredSize = adjustClusterSize(message.timeSinceLastUpdate, executionerNodes.size, message.avgExecutionTime.getOrElse(0), message.enqueued)
      log.info("SUBCLUSTER: {}| CoordinatorStats: executioners: {}:[{}], preferredExecutioners: {}", Array(uuid, executionerNodes.size, executionerNodes.keys.mkString(", "), preferredSize))

      if (executionerNodes.size < preferredSize) {
        val amountToAdd = preferredSize - executionerNodes.size
        log.info("SUBCLUSTER: {}| Adding: {} new executioners", uuid, amountToAdd)
        (0 until amountToAdd) foreach (i => {
          port += 1
          nodeProvider ! StartExecutionerNode(uuid, port)
        })
      } else {
        val execNodes = if (executionerNodes.size > preferredSize && preferredSize >= minExecutors) {
          val toShutDown = Math.min(executionerNodes.size - preferredSize, executionerNodes.size - minExecutors)

          log.info("SUBCLUSTER: {}| Removing: {} executioners", uuid, toShutDown)
          executionerNodes.filter(_._2.ref.isDefined).drop(toShutDown).toList
        } else {
          executionerNodes.toList
        }

        val execRefs: List[ActorRef] = execNodes.flatMap(_._2.ref)
        coordinatorNode match {
          case Some(node) =>
            node.ref match {
              case Some(coordinatorRef) =>
                coordinatorRef ! UseTheseExecutioners(execRefs)
              case None =>
                log.error("SUBCLUSTER: {}| No coordinator ref found!", uuid)
            }
          case None =>
            log.error("SUBCLUSTER: {}| No coordinator node active!", uuid)
        }

        val ports = execRefs.flatMap(_.path.address.port)
        val toShutDown = executionerNodes.filter(node => !ports.contains(node._1))
        executionerNodes --= toShutDown.keys
        toShutDown.flatMap(_._2.ref).foreach(_ ! ExecutionerShutdown(uuid))

      }

    case message: KeepExecutioners =>
      log.info("SUBCLUSTER: {}| KeepExecutioners: {}", uuid, message)
//      val ports = message.execs.flatMap(_.path.address.port)
//      val toShutDown = executionerNodes.filter(node => !ports.contains(node._1))
//      executionerNodes --= toShutDown.keys
//      toShutDown.flatMap(_._2.ref).foreach(_ ! ExecutionerShutdown(uuid))

    case s: String =>
      log.info("SUBCLUSTER: {}| Got {}", uuid, s)
    case SubscribeAck(Subscribe(_, None, `self`)) =>
      log.info("SUBCLUSTER: {}| subscribing", uuid)
    case MemberJoined(member) =>
      log.info("SUBCLUSTER: {}| MemberJoined: {}.", uuid, member)
    case MemberUp(member) =>
      log.info("SUBCLUSTER: {}| MemberUp: {}.", uuid, member)
      member.address.port match {
        case Some(memberPort) =>
          nodeTypeByPort(startingTcpPort, memberPort) match {
            case NodeType.Coordinator =>
              log.info("SUBCLUSTER: {}| Coordinator node joined subcluster", uuid)
              coordinatorNode = Some(NodeDetails(member, NodeType.Coordinator, None))
            case NodeType.Executioner =>
              log.info("SUBCLUSTER: {}| Executioner node joined subcluster", uuid)
              executionerNodes += (memberPort -> NodeDetails(member, NodeType.Executioner, None))
          }
        case None =>
          log.error("SUBCLUSTER: {}| Member without the port joined cluster: {}", uuid, member)
      }
    case MemberWeaklyUp(member) =>
      log.info("SUBCLUSTER: {}| MemberWeaklyUp: {}.", uuid, member)
    case MemberExited(member) =>
      log.info("SUBCLUSTER: {}| MemberExited: {}.", uuid, member)
    case MemberRemoved(member, previousState) =>
      if (previousState == MemberStatus.Exiting) {
        log.info("SUBCLUSTER: {}| Member {} Previously gracefully exited, REMOVED.", uuid, member)
      } else {
        log.info("SUBCLUSTER: {}| Member {} Previously downed after unreachable, REMOVED.", uuid, member)
      }
      member.address.port match {
        case Some(memberPort) =>
          nodeTypeByPort(startingTcpPort, memberPort) match {
            case NodeType.Coordinator =>
              log.info("SUBCLUSTER: {}| Coordinator node removed from cluster, starting new one!", uuid)
              nodeProvider ! StartCoordinatorNode(uuid, startingTcpPort + 1, httpPort)
            case NodeType.Executioner =>
              executionerNodes.get(memberPort) match {
                case Some(_) =>
                  log.info("SUBCLUSTER: {}| Executioner node on port: {} failed, starting new one!", uuid, memberPort)
                  executionerNodes -= memberPort
                  port += 1
                  nodeProvider ! StartExecutionerNode(uuid, port)

                  // send update to coordinator
                  coordinatorNode.flatMap(_.ref).foreach(_ ! UseTheseExecutioners(executionerNodes.flatMap(_._2.ref).toList))
                case None =>
                  log.info("SUBCLUSTER: {}| Executioner node on port: {} is removed after being scheduled for shutdown", uuid, memberPort)
              }
          }
        case None =>
          log.info("SUBCLUSTER: {}| Cannot extract port from member: {}", uuid, member)
      }
    case UnreachableMember(member) =>
      log.info("SUBCLUSTER: {}| UnreachableMember: {}.", uuid, member)
    case ReachableMember(member) =>
      log.info("SUBCLUSTER: {}| ReachableMember: {}.", uuid, member)

  }


  def nodeTypeByPort(startingPort: Int, port: Int) = if (startingPort + 1 == port) NodeType.Coordinator else NodeType.Executioner


  def adjustClusterSize(timeSinceLastStatusUpdate: Int, nodes: Int, avgExecutionTime: Int, enqueued: Int): Int = {
    log.info("adjustClusterSize: timeSinceLastUpate: {}, nodes: {}, avgExecutionTime: {}, enqueue: {}", timeSinceLastStatusUpdate, nodes, avgExecutionTime, enqueued)
    if (avgExecutionTime == 0) {
      if (enqueued == 0 || enqueued < nodes) {
        val value = Math.floor(nodes * (1 - CoordinatorActor.backOffRate)).toInt
        return value
      }
      return nodes
    }

    if (timeSinceLastStatusUpdate / avgExecutionTime * nodes * 0.8 * 0.7 > enqueued) {
      Math.floor(nodes * (1 - CoordinatorActor.backOffRate)).toInt
    } else if (timeSinceLastStatusUpdate / avgExecutionTime * 0.8 * nodes < enqueued) {
      Math.ceil(nodes * (1 + CoordinatorActor.rampUpRate)).toInt
    } else {
      nodes
    }
  }
}
