package com.jslambda.manager

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.jslambda.coordinator.CoordinatorActor.{AdjustClusterSize, ExecutionersRemoved}
import com.jslambda.manager.ClusterManager.{AddNewExecutioners, ExecutionerShutdown, RemoveExecs}
import com.jslambda.manager.NodeProvider.StartCluster
import com.jslambda.manager.SuperClusterManager.{CoordinatorJoined, CoordinatorRecognized, ExecutionerJoined, ExecutionerRecognized}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.collection.mutable.Set
import scala.concurrent.duration.FiniteDuration

object ClusterManager {
  def props(uuid: String, script: String, startingTcpPort: Int, httpPort: Int, minExecutors: Int) = Props(new ClusterManager(uuid, script, startingTcpPort, httpPort, minExecutors))

  case class AddNewExecutioners(additions: Int, uuid: String)

  case class RemoveExecs(execs: List[ActorRef])

  case class ExecutionerShutdown(uuid: String)

}

class ClusterManager(uuid: String, script: String, startingTcpPort: Int, httpPort: Int, minExecutors: Int) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  implicit val timeout: Timeout = FiniteDuration(10, java.util.concurrent.TimeUnit.SECONDS)

  val nodeProvider = context.actorOf(NodeProvider.props("node-provider", uuid, startingTcpPort, httpPort, minExecutors), "node-provider")
  nodeProvider ! StartCluster(minExecutors, uuid)

  var coordinatorRef: Option[ActorRef] = None
  var executionerRefs: Set[ActorRef] = Set()

  override def receive: Receive = {
    case message: CoordinatorJoined =>
      log.info("CoordinatorJoined: {}", message)
      coordinatorRef = Some(message.actorRef)

      message.actorRef ! CoordinatorRecognized(uuid)

    case message: ExecutionerJoined =>
      log.info("ExecutionerJoined: {}", message)
      executionerRefs += message.actorRef

      message.actorRef ! ExecutionerRecognized(uuid)

    case message: AdjustClusterSize =>
      log.info("AdjustClusterSize: {}", message)
      if (executionerRefs.size < message.preferredSize) {
        nodeProvider ! AddNewExecutioners(message.preferredSize - executionerRefs.size, uuid)
      } else if (executionerRefs.size > message.preferredSize && message.preferredSize > minExecutors) {
        val toShutDown = Math.min(executionerRefs.size - message.preferredSize, executionerRefs.size - minExecutors)
        val execs = executionerRefs.take(toShutDown).toList
        execs.foreach(executionerRefs.remove(_))

        coordinatorRef.foreach(ref => pipe(ref ? RemoveExecs(execs)) to self)
      }

    case message: ExecutionersRemoved =>
      log.info("ExecutionersRemoved: {}", message)
      message.execs.foreach(_ ! ExecutionerShutdown(uuid))

    //    case message: AddNewExecutioner =>
    //      nodeProvider forward message
    //      coordinatorRef match {
    //        case Some(coordinator) =>
    //          coordinator forward message
    //        case None =>
    //          log.error("Executioners joined to cluster without coordinator!")
    //      }
  }

}
