package com.jslambda.executioner

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import com.jslambda.coordinator.CoordinatorActor.ConnectToManager
import com.jslambda.coordinator.ExecuteExpression
import com.jslambda.executioner.ScriptExecutioner.{ExecutionDone, ExecutionResult, ManagedExecuteScript}
import com.jslambda.manager.SubClusterManager.ExecutionerShutdown
import com.jslambda.manager.SuperClusterManager._
import javax.script.{Invocable, ScriptEngineManager}

import scala.concurrent.duration.FiniteDuration

object ScriptExecutioner {
  def props(name: String, script: String) = Props(new ScriptExecutioner(name, script))

  case class ExecutionResult(result: Option[String])

  case class ExecutionDone(millis: Long)

  case class ManagedExecuteScript(request: ExecuteExpression, sender: ActorRef, createdTime: Long)


}

class ScriptExecutioner(val uuid: String, val script: String) extends Actor with ActorLogging {
  implicit val ec = context.dispatcher

  val engine = new ScriptEngineManager().getEngineByName("nashorn")

  engine.eval(script)

  val invocable: Invocable = engine.asInstanceOf[Invocable]

  private var recognized = false

  var parent: ActorRef = null

  private val mediator = DistributedPubSub(context.system).mediator

  mediator ! Subscribe(uuid + "-bus", self)

  val connectToManagerSchedule: Cancellable = context.system.scheduler.schedule(FiniteDuration(2, TimeUnit.SECONDS), FiniteDuration(1, TimeUnit.SECONDS), self, ConnectToManager())
  var connectionAttempts = 0

  override def receive: Receive = {
    case _: ConnectToManager =>
      connectionAttempts += 1
      log.info("SUBCLUSTER: {}| ConnectToManager by executioner", uuid)
      if (!recognized) {
        log.info("SUBCLUSTER: {}| Publishing ExecutionerJoined: {}", uuid, ExecutionerJoined(uuid, self))
        mediator ! Publish(uuid + "-bus", ExecutionerJoined(uuid, self))
      } else {
        log.info("SUBCLUSTER: {}| Executioner node recognized, shutting down scheduled task", uuid)
        connectToManagerSchedule.cancel()
      }

    case message: ExecutionerRecognized =>
      log.info("SUBCLUSTER: {}| ExecutionerRecognized: {}", uuid, message)
      recognized = true


    case SubscribeAck(Subscribe("cluster-bus", None, `self`)) =>
      log.info("SubscribeAck")

    case message: ManagedExecuteScript => {
      log.info("ManagedExecuteScript: {}", message.request.function)
      if (Array("kill_executioner").contains(message.request.function)) {
        val cluster = Cluster(context.system)
        cluster.leave(cluster.selfAddress)
      } else {
        parent = sender
        val start = System.currentTimeMillis
        val result = invokeFunction(message.request.function, message.request.params)
        if (result == null) {
          sender forward ExecutionResult(Some(null))
        } else {
          result match {
            case array: Array[Integer] =>
              val asString = array.mkString(",")
              message.sender forward ExecutionResult(Some(s"[$asString]"))
            case value =>
              message.sender forward ExecutionResult(Some(value.toString))
          }
        }
        parent ! ExecutionDone(Math.max(System.currentTimeMillis - start, 1))
      }
    }

    case message: ExecutionerShutdown =>
      log.info("SUBCLUSTER: {}| ExecutionerShutdown: {}", uuid, message)
      val cluster = Cluster(context.system)
      cluster.leave(cluster.selfAddress)
    //      self ! PoisonPill
  }

  def invokeFunction(function: String, params: String): Object = {
    if (params.startsWith("[")) {
      val values = params.substring(1, params.length - 2)
      val intArray = values.split(",").map(part => Integer.valueOf(part.trim.toInt)).array
      invocable.invokeFunction(function, intArray)
    } else {
      val convertedParams = params.split(",").map(part => Integer.valueOf(part.trim.toInt)).array
      if (convertedParams.length == 0) {
        return invocable.invokeFunction(function)
      } else if (convertedParams.length == 1) {
        return invocable.invokeFunction(function, convertedParams(0))
      } else if (convertedParams.length == 2) {
        return invocable.invokeFunction(function, convertedParams(0), convertedParams(1))
      } else if (convertedParams.length == 3) {
        return invocable.invokeFunction(function, convertedParams(0), convertedParams(1), convertedParams(2))
      } else if (convertedParams.length == 4) {
        return invocable.invokeFunction(function, convertedParams(0), convertedParams(1), convertedParams(2), convertedParams(3))
      } else if (convertedParams.length == 5) {
        return invocable.invokeFunction(function, convertedParams(0), convertedParams(1), convertedParams(2), convertedParams(3), convertedParams(4))
      }
      null
    }
  }

  def convertParameters(str: String): Array[Integer] = {
    if (str.startsWith("[")) {
      val values = str.substring(1, str.length - 2)
      values.split(",").map(part => Integer.valueOf(part.trim.toInt)).array
    } else {
      str.split(",").map(part => Integer.valueOf(part.trim.toInt)).array
    }
  }

}
