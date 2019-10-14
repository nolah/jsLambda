package com.jslambda.coordinator

import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import com.jslambda.coordinator.CoordinatorActor._
import com.jslambda.executioner.ScriptExecutioner.{ExecutionDone, ManagedExecuteScript}
import com.jslambda.manager.SubClusterManager.RemoveExecs
import com.jslambda.manager.SuperClusterManager.{CoordinatorJoined, CoordinatorRecognized, ExecutionerJoined}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration


object CoordinatorActor {
  def props(name: String, uuid: String) = Props(new CoordinatorActor(name, uuid))

  case class CheckStatus()

  case class ExecutionerDetails(free: Boolean, lastExecution: ZonedDateTime)

  case class AdjustClusterSize(preferredSize: Int, uuid: String)

  case class ExecutionersRemoved(numberOfExecsRemoved: Int, execs: List[ActorRef])

  case class ConnectToManager()

  val rampUpRate = 0.25
  val backOffRate = 0.1
  val avgStartupTime = 5000

}

class CoordinatorActor(name: String, uuid: String) extends Actor with ActorLogging {
  implicit val ec = context.dispatcher

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

  mediator ! Subscribe(uuid + "-bus", self)

  val connectToManagerSchedule: Cancellable = context.system.scheduler.schedule(FiniteDuration(1, TimeUnit.SECONDS), FiniteDuration(1, TimeUnit.SECONDS), self, ConnectToManager())

  override def receive: Receive = {
    case _: ConnectToManager =>
      log.info("ConnectToManager")
      if (!recognized) {
        log.info("Publishing CoordinatorJoined: {}", CoordinatorJoined(uuid, self))
        mediator ! Publish(uuid + "-bus", CoordinatorJoined(uuid, self))
      } else {
        log.info("Node recognized, shutting down scheduled task")
        connectToManagerSchedule.cancel()
      }


    case _: CheckStatus =>
      log.info("CheckStatus")

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

      // calculate preferred number of executioners
      val nodes = executioners.size
      val enqueued = pendingExecutions.length
      val timeSinceLastStatusUpdate = lastStatusUpdate.map(x => Math.max(System.currentTimeMillis() - x, 5000).toInt).getOrElse(5000)

      val preferredNumberOfExecutioners = adjustClusterSize(timeSinceLastStatusUpdate, nodes, avgExecutionTimeInMs.getOrElse(0), enqueued)

      clusterManager ! AdjustClusterSize(preferredNumberOfExecutioners, uuid)

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
      executioners = executioners.empty
      message.executioners.foreach(e => executioners += (e -> ExecutionerDetails(true, ZonedDateTime.now())))

    case SubscribeAck(Subscribe("cluster-bus", None, `self`)) =>
      log.info("SubscribeAck")

    case message: ExecutionerJoined =>
      log.info("ExecutionerJoined: {}", message)
      executioners += (message.actorRef -> ExecutionerDetails(true, ZonedDateTime.now()))
      log.info("ExecutionerJoined: new state: {}", executioners.size)

    case message: ExecuteExpression => {
      log.info("ExecuteExpression, {}", message.function)
      if (message.function.equals("shutdown_actor_coordinator")) {
        throw new RuntimeException
      } else if (message.function.equals("shutdown_actor_coordinator")) {
        val cluster = Cluster(context.system)
        cluster.leave(cluster.selfAddress)
      }
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


    if (avgExecutionTime == 0 || timeSinceLastStatusUpdate / avgExecutionTime * nodes * 0.8 * 0.7 > enqueued) {
      Math.floor(nodes * (1 - CoordinatorActor.backOffRate)).toInt
    } else if (timeSinceLastStatusUpdate / avgExecutionTime * 0.8 * nodes < enqueued) {
      Math.ceil(nodes * (1 + CoordinatorActor.rampUpRate)).toInt
    } else {
      nodes
    }

  }


}
