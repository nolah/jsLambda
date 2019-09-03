package com.jslambda.actor

import akka.actor.{Actor, ActorLogging, Props}
import com.jslambda.actor.ScriptCollection.{ExecuteScript, ExecutionResult}
import com.jslambda.actor.ScriptExecutionCoordinator.{ExecutionDone, ManagedExecuteScript}
import javax.script.{Invocable, ScriptEngineManager}

object ScriptExecutor {
  def props(name: String, script: String) = Props(new ScriptExecutor(name, script))
}

class ScriptExecutor(name: String, script: String) extends Actor with ActorLogging {

  val engine = new ScriptEngineManager().getEngineByName("nashorn")
  engine.eval(script)
  val invocable = engine.asInstanceOf[Invocable]

  override def receive: Receive = {

    case message: ManagedExecuteScript => {
      val start = System.currentTimeMillis
      val result = invocable.invokeFunction(message.request.function, convertParameters(message.request.parameters))
      if (result == null) {
        message.sender forward ExecutionResult(Some(null), None)
      } else {
        result match {
          case array: Array[Int] =>
            val asString = array.mkString(",")
            message.sender forward ExecutionResult(Some(s"[$asString]"), None)
          case value =>
            message.sender forward ExecutionResult(Some(value.toString), None)
        }
      }

      context.parent ! new ExecutionDone
    }
  }

  def convertParameters(str: String): Array[Int] = {
    if (str.startsWith("[")) {
      val values = str.substring(1, str.length - 2)
      values.split(",").map(part => part.trim.toInt).array
    } else {
      str.split(",").map(part => part.toInt).array
    }
  }

}
