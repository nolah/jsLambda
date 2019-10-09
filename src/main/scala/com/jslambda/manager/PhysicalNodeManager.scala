package com.jslambda.manager

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.Member

import scala.collection.mutable.ListBuffer

object PhysicalNodeManager {
  def props(name: String) = Props(new PhysicalNodeManager(name))
}

class PhysicalNodeManager(name: String) extends Actor with ActorLogging {

  var coordinators: ListBuffer[Member] = new ListBuffer
  var workers: ListBuffer[Member] = new ListBuffer

  override def receive: Receive = {
    case message: MemberUp =>
      if (message.member.roles.contains("coordinator")) {
        coordinators += message.member
      } else if (message.member.roles.contains("executioner")) {
        workers += message.member
      }
  }

}
