package com.jslambda.coordinator

import java.time.ZonedDateTime

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import com.jslambda.coordinator.CoordinatorActor.{AdjustClusterSize, CheckStatus, ExecutionerDetails, ExecutionersRemoved}
import com.jslambda.executioner.ScriptExecutioner.{ExecutionDone, ManagedExecuteScript}
import com.jslambda.manager.ClusterManager.RemoveExecs
import com.jslambda.manager.SuperClusterManager.{CoordinatorJoined, CoordinatorRecognized, ExecutionerJoined, NewMemberIdentifyYourself}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object CoordinatorActor {
  def props(name: String, uuid: String) = Props(new CoordinatorActor(name, uuid))

  case class CheckStatus()

  case class ExecutionerDetails(free: Boolean, lastExecution: ZonedDateTime)

  case class AdjustClusterSize(preferredSize: Int, uuid: String)

  case class ExecutionersRemoved(numberOfExecsRemoved: Int, execs: List[ActorRef])

  val rampUpRate = 0.25
  val backOffRate = 0.1
  val avgStartupTime = 5000

}

class CoordinatorActor(name: String, uuid: String) extends Actor with ActorLogging {
  private var recognized = false

  var executioners = Map[ActorRef, ExecutionerDetails]()

  var pendingExecutions = mutable.Queue[ManagedExecuteScript]()

  var executionsDone = ListBuffer[ExecutionDone]()

  var avgExecutionTimeInMs: Option[Int] = None

  var clusterManager: ActorRef = _

  var next = 0

  var lastStatusUpdate: Option[Long] = None

  var startNewExecutionerTime: Option[Long] = None

  private val mediator = DistributedPubSub(context.system).mediator

  mediator ! Subscribe("cluster-bus", self)

  override def receive: Receive = {
    case _: CheckStatus =>
      log.info("Status check")

      // drop stale executions
      pendingExecutions = dropStaleExecutions(pendingExecutions)

      if (executionsDone.nonEmpty) {
        // calculate avg execution time
        val totalExecutionTime: Long = executionsDone.foldLeft(0l) { (a, b) => a + b.millis }
        avgExecutionTimeInMs = Some(totalExecutionTime.intValue() / executionsDone.size)
        executionsDone.clear
      } else {
        avgExecutionTimeInMs = None
      }

      if (avgExecutionTimeInMs.isDefined) {
        // calculate preferred number of executioners
        val nodes = executioners.size
        val enqueued = pendingExecutions.length
        val timeSinceLastStatusUpdate = lastStatusUpdate.map(x => Math.max(System.currentTimeMillis() - x, 5000).toInt).getOrElse(5000)

        val preferredNumberOfExecutioners = adjustClusterSize(timeSinceLastStatusUpdate, nodes, avgExecutionTimeInMs.get, enqueued)

        clusterManager ! AdjustClusterSize(preferredNumberOfExecutioners, uuid)
      }

      lastStatusUpdate = Some(System.currentTimeMillis)
    case message: RemoveExecs =>
      log.info("RemoveExecs: {}", message)
      log.info("RemoveExecs: current state: {}", executioners.size)
      val oldSize = executioners.size
      executioners = executioners.filter(x => !message.execs.contains(x._1))
      sender forward ExecutionersRemoved(oldSize - executioners.size, message.execs)
      log.info("RemoveExecs: after removal: {}", executioners.size)

    case message: CoordinatorRecognized =>
      log.info("CoordinatorRecognized: {}", message)
      recognized = true
      clusterManager = sender

    case SubscribeAck(Subscribe("cluster-bus", None, `self`)) =>
      log.info("SubscribeAck")

    case _: NewMemberIdentifyYourself =>
      if (!recognized) {
        log.info("Publishing CoordinatorJoined: {}", CoordinatorJoined(uuid, self))
        mediator ! Publish("cluster-bus", CoordinatorJoined(uuid, self))
      }

    case message: ExecutionerJoined =>
      log.info("ExecutionerJoined: {}", message)
      executioners += (message.actorRef -> ExecutionerDetails(true, ZonedDateTime.now()))
      log.info("ExecutionerJoined: new state: {}", executioners.size)
    //      if (startNewExecutionerTime.isDefined) {
    //        log.info("Executioner startup took: {}", System.currentTimeMillis - startNewExecutionerTime.get)
    //        log.info("Executioner startup took: {}", System.currentTimeMillis - startNewExecutionerTime.get)
    //        log.info("Executioner startup took: {}", System.currentTimeMillis - startNewExecutionerTime.get)
    //        log.info("Executioner startup took: {}", System.currentTimeMillis - startNewExecutionerTime.get)
    //        log.info("Executioner startup took: {}", System.currentTimeMillis - startNewExecutionerTime.get)
    //        log.info("Executioner startup took: {}", System.currentTimeMillis - startNewExecutionerTime.get)
    //      }

    case message: ExecuteExpression => {
      //      if (pendingExecutions.length > 2 * executioners.size) {
      //    // ask for more executioners
      //      }

      log.info("ExecuteExpression, {}", message)
      findFreeExecutioner(executioners) match {
        case Some((executioner, _)) =>
          executioners += (executioner -> ExecutionerDetails(false, ZonedDateTime.now()))
          executioner ! ManagedExecuteScript(message, sender, System.currentTimeMillis)
        case None =>
          pendingExecutions.enqueue(ManagedExecuteScript(message, sender, System.currentTimeMillis))
      }
    }
    case message: ExecutionDone =>
      log.info("ExecutionDone: {}", message)
      executionsDone += message
      if (pendingExecutions.isEmpty) {
        executioners += (sender -> ExecutionerDetails(true, ZonedDateTime.now()))
      } else {
        val execution = pendingExecutions.dequeue
        sender ! execution
      }
  }

  def findFreeExecutioner(executioners: Map[ActorRef, ExecutionerDetails]): Option[(ActorRef, ExecutionerDetails)] = {
    if (executioners.isEmpty) {
      log.error("Exectioners are empty!")
    }
    executioners.find(tuple => tuple._2.free)
  }

  def dropStaleExecutions(executions: mutable.Queue[ManagedExecuteScript]) = {
    val currentTime = System.currentTimeMillis
    executions.filter(ex => (currentTime - ex.createdTime) < 10000)
  }

  def adjustClusterSize(timeSinceLastStatusUpdate: Int, nodes: Int, avgExecutionTime: Int, enqueued: Int): Int = {
    log.info("adjustClusterSize: timeSinceLastUpate: {}, nodes: {}, avgExecutionTime: {}, enqueue: {}", timeSinceLastStatusUpdate, nodes, avgExecutionTime, enqueued)
    if (avgExecutionTime == 0) {
      return nodes
    }

    if (timeSinceLastStatusUpdate / avgExecutionTime * 0.8 * nodes < enqueued) {
      Math.ceil(nodes * (1 + CoordinatorActor.rampUpRate)).toInt
    } else if (timeSinceLastStatusUpdate / avgExecutionTime * nodes * 0.8 * 0.7 > enqueued) {
      Math.floor(nodes * (1 - CoordinatorActor.backOffRate)).toInt
    } else {
      nodes
    }

  }


}
