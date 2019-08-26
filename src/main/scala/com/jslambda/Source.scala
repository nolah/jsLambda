package com.jslambda

import akka.actor.{Actor, ActorRef, Props}

object Source {
  def props(name: String, source: String) = Props(new Source(name, source))

  def name(name: String) = name

  case class Expression(expression: String, resultId: String)

}

class Source(name: String, source: String) extends Actor {

  import Source._

  val base = 3

  var map: Map[Int, ActorRef] = Map()
  (0 to base).foreach(i => map += (i -> context.actorOf(Executor.props(source))))

  var order = 0

  override def receive: Receive = {
    case expression: Expression =>
      val exe = map(order)
      exe forward expression
      order = (order + 1) % base
  }


  def forwardMessage(expression: Source.Expression)(executor: ActorRef) =
    executor forward expression

  def createAndForward(expression: Expression) = {
    createExecutor() forward expression
  }

  def createExecutor() =
    context.actorOf(Executor.props(source))

}
