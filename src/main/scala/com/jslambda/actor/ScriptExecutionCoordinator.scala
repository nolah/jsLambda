package com.jslambda.actor

import java.time.ZonedDateTime

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.jslambda.actor.ScriptCollection.ExecuteScript
import com.jslambda.actor.ScriptExecutionCoordinator.{ExecutionDone, ExecutionerDetails, ManagedExecuteScript}

import scala.collection.mutable

object ScriptExecutionCoordinator {
  def props(name: String, script: String) = Props(new ScriptExecutionCoordinator(name, script))

  class ExecutionDone

  case class ManagedExecuteScript(request: ExecuteScript, sender: ActorRef, createdTime: Long)

  case class ExecutionerDetails(free: Boolean, lastExecution: ZonedDateTime)

}

class ScriptExecutionCoordinator(name: String, script: String) extends Actor with ActorLogging {


  var executioners = Map[ActorRef, ExecutionerDetails]()
  executioners += (context.actorOf(ScriptExecutor.props("first", script), "first") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("second", script), "second") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("third", script), "third") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("4", script), "4") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("5", script), "5") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("6", script), "6") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("7", script), "7") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("8", script), "8") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("9", script), "9") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("10", script), "10") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("11", script), "11") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("12", script), "12") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("13", script), "13") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("14", script), "14") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("15", script), "15") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("16", script), "16") -> ExecutionerDetails(true, ZonedDateTime.now()))
  executioners += (context.actorOf(ScriptExecutor.props("17", script), "17") -> ExecutionerDetails(true, ZonedDateTime.now()))

  var pendingExecutions = mutable.Queue[ManagedExecuteScript]()

  override def receive: Receive = {
    case message: ExecuteScript => {
//      if (pendingExecutions.length > 2 * executioners.size) {
//
//      }
      findFreeExecutioner(executioners) match {
        case Some((executioner, _)) =>
          executioners += (executioner -> ExecutionerDetails(false, ZonedDateTime.now()))
          executioner ! ManagedExecuteScript(message, sender, System.currentTimeMillis)
        case None =>
          pendingExecutions.enqueue(ManagedExecuteScript(message, sender, System.currentTimeMillis))
      }
    }
    case _: ExecutionDone =>
      pendingExecutions = dropStaleExecutions(pendingExecutions)
      if (pendingExecutions.isEmpty) {
        executioners += (sender -> ExecutionerDetails(true, ZonedDateTime.now()))
      } else {
        val execution = pendingExecutions.dequeue
        sender ! execution
      }
  }

  def findFreeExecutioner(executioners: Map[ActorRef, ExecutionerDetails]): Option[(ActorRef, ExecutionerDetails)] = {
    executioners.find(tuple => tuple._2.free)
  }

  def dropStaleExecutions(executions:mutable.Queue[ManagedExecuteScript]) = {
    val currentTime = System.currentTimeMillis
    executions.filter(ex => (currentTime - ex.createdTime) < 10000)
  }

}
