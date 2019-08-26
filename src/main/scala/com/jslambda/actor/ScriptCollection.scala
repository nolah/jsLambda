package com.jslambda.actor

import java.util.UUID

import akka.actor.{Actor, Props}
import com.jslambda.actor.ScriptCollection.{ExecuteScript, ExecutionResult, RegisterScript, ScriptRegistered}

object ScriptCollection {
  def props(name: String) = Props(new ScriptCollection(name))

  case class ExecuteScript(uuid: String, expression: String)

  case class ExecutionResult(result: Option[String], error: Option[String])



  case class RegisterScript(script: String)

  case class ScriptRegistered(id: String)


  case class ExecuteExpression(expression: String)
  case class ExpressionExecuted(result: String)


}

class ScriptCollection(name: String) extends Actor {

  override def receive: Receive = {
    case RegisterScript(script) =>
      val uuid = UUID.randomUUID().toString
      context.actorOf(Script.props(uuid, script).withDispatcher("my-thread-pool-dispatcher"), uuid)
      sender forward ScriptRegistered(uuid)

    case message: ExecuteScript =>
      context.child(message.uuid) match {
        case None =>
          sender forward ExecutionResult(None, Some("NotFound"))
        case Some(script) =>
          script forward message
      }
  }

}
