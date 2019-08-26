package com.jslambda

import java.util.{Date, UUID}

import akka.actor.{Actor, ActorRef, Props}
object SourceSupervisor {
  def props(name: String) = Props(new SourceSupervisor(name))
  def name(name: String) = name

  case class AddSource(source: String)
  case class SourceAdded(name: String)
  case class ExecuteExpression(sourceName: String, expression: String)
}
class SourceSupervisor(name: String) extends Actor{

  import SourceSupervisor._


  override def receive: Receive = {
    case message : AddSource =>
      val uuid = UUID.randomUUID().toString
      context.actorOf(Source.props(uuid, message.source), uuid)

      sender() ! SourceAdded(uuid)

    case message: ExecuteExpression =>
      context.child(Source.name(message.sourceName)).get forward  Source.Expression(message.expression, s"${message.sourceName}${UUID.randomUUID().toString}")
  }


}
