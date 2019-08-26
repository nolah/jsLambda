package com.jslambda

import java.util.Date

import org.mozilla.javascript.Context
import akka.actor.{Actor, ActorRef, PoisonPill, Props}


object Executor {
  def props(source: String) = Props(new Executor(source))

}

class Executor(source: String) extends Actor {

  import Source._

//  val simulator: ActorRef = context.actorOf(Simulator.props("simulator", "sourceSupervisor"), "simulator")


  val jscontext = Context.enter()
  val scope = jscontext.initStandardObjects()
  jscontext.evaluateString(scope, source, "__script.js", 1, null)
  Context.exit()



  override def receive: Receive = {
    case expression: Expression =>

      try {

        Context.enter()
        val result = jscontext.evaluateString(scope, expression.expression, "inline", 1, null).toString
//        println(s"Result id: ${expression.resultId}, result=$result")
        Context.exit()
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
      sender() ! Simulator.JobDone()
//      println(new Date())
//      self ! PoisonPill
  }


}
