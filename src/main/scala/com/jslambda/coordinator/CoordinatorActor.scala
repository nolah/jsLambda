package com.jslambda.coordinator

import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import com.jslambda.coordinator.CoordinatorActor._
import com.jslambda.executioner.ScriptExecutioner.{ExecutionDone, ManagedExecuteScript}
import com.jslambda.manager.SubClusterManager.UseTheseExecutioners
import com.jslambda.manager.SuperClusterManager.{CoordinatorJoined, CoordinatorRecognized, ExecutionerJoined}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration


object CoordinatorActor {
  def props(name: String, uuid: String) = Props(new CoordinatorActor(name, uuid))

  case class CheckStatus()

  case class ExecutionerDetails(free: Boolean, lastExecution: ZonedDateTime)

  case class CoordinatorStats(nodes: Int, enqueued: Int, timeSinceLastUpdate: Int, avgExecutionTime: Option[Int], uuid: String)

  case class KeepExecutioners(numberOfExecsRemoved: Int, execs: List[ActorRef])

  case class ConnectToManager()

  val rampUpRate = 0.25
  val backOffRate = 0.1
  val avgStartupTime = 5000

}

class CoordinatorActor(name: String, uuid: String) extends Actor with ActorLogging {
  implicit val ec = context.dispatcher

  private var recognized = false

  var executioners = mutable.Map[ActorRef, ExecutionerDetails]()

  var pendingExecutions = mutable.Queue[ManagedExecuteScript]()

  var executionsDone = ListBuffer[ExecutionDone]()

  var avgExecutionTimeInMs: Option[Int] = None

  var clusterManager: Option[ActorRef] = None

  var next = 0

  var lastStatusUpdate: Option[Long] = None

  var startNewExecutionerTime: Option[Long] = None

  private val mediator = DistributedPubSub(context.system).mediator

  mediator ! Subscribe(uuid + "-bus", self)

  val connectToManagerSchedule: Cancellable = context.system.scheduler.schedule(FiniteDuration(1, TimeUnit.SECONDS), FiniteDuration(1, TimeUnit.SECONDS), self, ConnectToManager())

  override def receive: Receive = {
    case _: ConnectToManager =>
      log.info("SUBCLUSTER: {}| ConnectToManager by coordinator", uuid)
      if (!recognized) {
        log.info("SUBCLUSTER: {}| Publishing CoordinatorJoined: {}", uuid, CoordinatorJoined(uuid, self))
        mediator ! Publish(uuid + "-bus", CoordinatorJoined(uuid, self))
      } else {
        log.info("SUBCLUSTER: {}| Coordinator node recognized, shutting down scheduled task", uuid)
        connectToManagerSchedule.cancel()
      }


    case _: CheckStatus =>

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

      log.info("SUBCLUSTER: {}| CheckStatus: enqueued: {}, avgExecution: {}, executioners: {}:[{}]", Array(uuid, enqueued, avgExecutionTimeInMs.getOrElse(0), nodes, executioners.keys.flatMap(_.path.address.port).mkString(", ")))

      clusterManager.foreach(_ ! CoordinatorStats(nodes, enqueued, timeSinceLastStatusUpdate, avgExecutionTimeInMs, uuid))

      lastStatusUpdate = Some(System.currentTimeMillis)

    case message: UseTheseExecutioners =>
      log.info("SUBCLUSTER: {}| UseTheseExecutioners: [{}], current state: [{}]", uuid, message.execs.flatMap(_.path.address.port), executioners.keys.flatMap(_.path.address.port))
      val oldSize = executioners.size
      val previousExecutioners = executioners
      executioners.clear()
      executioners ++= message.execs.map(e => e -> previousExecutioners.getOrElse(e, ExecutionerDetails(true, ZonedDateTime.now())))
      sender forward KeepExecutioners(oldSize - executioners.size, message.execs)
      log.info("SUBCLUSTER: {}| UseTheseExecutioners: after update: [{}]", uuid, executioners.keys.flatMap(_.path.address.port))

    case message: CoordinatorRecognized =>
      log.info("SUBCLUSTER: {}| CoordinatorRecognized: {}", uuid, message)
      recognized = true
      clusterManager = Some(sender)
      executioners = executioners.empty
      message.executioners.foreach(e => executioners += (e -> ExecutionerDetails(true, ZonedDateTime.now())))

    case SubscribeAck(Subscribe("cluster-bus", None, `self`)) =>
      log.info("SubscribeAck")

    case message: ExecuteExpression => {
      log.info("ExecuteExpression, {}", message.function)
      if (Array("kill_coordinator").contains(message.function)) {
        val cluster = Cluster(context.system)
        cluster.leave(cluster.selfAddress)
      } else {
        findFreeExecutioner(executioners) match {
          case Some((executioner, _)) =>
            executioners += (executioner -> ExecutionerDetails(false, ZonedDateTime.now()))
            executioner ! ManagedExecuteScript(message, sender, System.currentTimeMillis)
          case None =>
            pendingExecutions.enqueue(ManagedExecuteScript(message, sender, System.currentTimeMillis))
        }
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

  def findFreeExecutioner(executioners: mutable.Map[ActorRef, ExecutionerDetails]): Option[(ActorRef, ExecutionerDetails)] = {
    if (executioners.isEmpty) {
      log.error("Exectioners are empty!")
    }
    executioners.find(tuple => tuple._2.free)
  }

  def dropStaleExecutions(executions: mutable.Queue[ManagedExecuteScript]) = {
    val currentTime = System.currentTimeMillis
    executions.filter(ex => (currentTime - ex.createdTime) < 10000)
  }


}
