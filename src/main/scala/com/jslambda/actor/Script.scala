package com.jslambda.actor

import akka.actor.{Actor, ActorRef, Props}
import com.jslambda.actor.ScriptCollection.{ExecuteScript, ExecutionResult}

object Script {
  def props(name: String, script: String) = Props(new Script(name, script))

  def name(name: String): String = name

}

class Script(val name: String, val script: String) extends Actor {

  val jsContext = org.mozilla.javascript.Context.enter()
  val scope = jsContext.initStandardObjects()
  jsContext.evaluateString(scope, script, "jsCode", 0, null)

  override def receive: Receive = {

    case message: ExecuteScript => {

      val a = 3
      val result = jsContext.evaluateString(scope, message.expression, "inline", 1, null).toString
      println(result)
      val resultAsString = if (result == null) null else result.toString
      sender forward ExecutionResult(Some(resultAsString), None)
    }
  }

}
