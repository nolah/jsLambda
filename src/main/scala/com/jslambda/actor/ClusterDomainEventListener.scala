package com.jslambda.actor

import akka.actor.{Actor, ActorLogging}
import akka.cluster.{Cluster, MemberStatus}
import akka.cluster.ClusterEvent._

class ClusterDomainEventListener extends Actor
  with ActorLogging {
  Cluster(context.system).subscribe(self, classOf[ClusterDomainEvent])

  def receive ={
    case MemberJoined(member) =>
      log.info("MemberJoined: {}.", member)
    case MemberUp(member) =>
      log.info("MemberUp: {}.", member)
    case MemberWeaklyUp(member) =>
      log.info("MemberWeaklyUp: {}.", member)
    case MemberExited(member)=>
      log.info("MemberExited: {}.", member)
    case MemberRemoved(member, previousState)=>
      if(previousState == MemberStatus.Exiting) {
        log.info("Member {} Previously gracefully exited, REMOVED.", member)
      } else {
        log.info("Member {} Previously downed after unreachable, REMOVED.", member)
      }
    case UnreachableMember(member) =>
      log.info("UnreachableMember: {}.", member)
    case ReachableMember(member) =>
      log.info("ReachableMember: {}.", member)
    case state: CurrentClusterState =>
      log.info("CurrentClusterState: {}.", state)
  }
  override def postStop(): Unit = {
    Cluster(context.system).unsubscribe(self)
    super.postStop()
  }
}
