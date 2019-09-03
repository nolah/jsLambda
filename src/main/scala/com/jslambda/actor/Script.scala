package com.jslambda.actor

import akka.actor.{Actor, ActorRef, Props}
import com.jslambda.actor.ScriptCollection.{ExecuteScript, ExecutionResult}

object Script {
  def props(name: String, script: String) = Props(new Script(name, script))

  def name(name: String): String = name

}

class Script(val name: String, val script: String) extends Actor {

  val coordinator = context.actorOf(ScriptExecutionCoordinator.props("coordinator", script), "coordinator")

  override def receive: Receive = {

    case message: ExecuteScript => {
      coordinator forward message
    }
  }

}
