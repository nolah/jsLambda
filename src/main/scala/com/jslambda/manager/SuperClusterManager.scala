package com.jslambda.manager

import java.io.{File, FileWriter}
import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.{Cluster, MemberStatus}
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import com.jslambda.coordinator.{RegisterScript, ScriptRegistered}
import com.jslambda.manager.SuperClusterManager.{CoordinatorJoined, ExecutionerJoined, NewMemberIdentifyYourself}

object SuperClusterManager {

  def props(name: String, storageDir: String) = Props(new SuperClusterManager(name, storageDir))

  case class CoordinatorJoined(uuid: String, actorRef: ActorRef)

  case class CoordinatorRecognized(uuid: String)

  case class NewMemberIdentifyYourself(ninja: String)

  case class ExecutionerJoined(uuid: String, actorRef: ActorRef)

  case class ExecutionerRecognized(uuid: String)

}

class SuperClusterManager(name: String, storageDir: String) extends Actor with ActorLogging {
  private val mediator = DistributedPubSub(context.system).mediator

  mediator ! Subscribe("cluster-bus", self)

  Cluster(context.system).subscribe(self, classOf[ClusterDomainEvent])

  val physicalNodeManager = context.actorOf(PhysicalNodeManager.props("physical-node-manager"), "physical-node-manager")

  def receive = {
    case s: String =>
      log.info("Got {}", s)
    case SubscribeAck(Subscribe("content", None, `self`)) =>
      log.info("subscribing")
    case MemberJoined(member) =>
      log.info("MemberJoined: {}.", member)
    case MemberUp(member) =>
      log.info("MemberUp: {}.", member)
      physicalNodeManager forward MemberUp(member)
      Thread.sleep(1000)
      log.info("Publishing NewMemberIdentifyYourself")
      mediator ! Publish("cluster-bus", NewMemberIdentifyYourself("42"))
    case MemberWeaklyUp(member) =>
      log.info("MemberWeaklyUp: {}.", member)
    case MemberExited(member) =>
      log.info("MemberExited: {}.", member)
    case MemberRemoved(member, previousState) =>
      if (previousState == MemberStatus.Exiting) {
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

    case message: RegisterScript =>
      sender forward ScriptRegistered(createClusterManager(message.script, message.minimumNodes))
    case message: CoordinatorJoined =>
      context.child(message.uuid) match {
        case Some(child) =>
          log.info("CoordinatorJoined: {}", message)
          child forward message
        case None => log.error("Unknown coordinator is trying to join cluster!")
      }
    case message: ExecutionerJoined =>
      context.child(message.uuid) match {
        case Some(child) =>
          log.info("ExecutionerJoined: {}", message)
          child forward message
        case None => log.error("Unknown executioner is trying to join cluster!")
      }

  }


  def createClusterManager(script: String, minExecutors: Int): String = {
    val uuid = UUID.randomUUID().toString
    val file = new File(storageDir + uuid + ".script")
    val fileWriter = new FileWriter(file)
    fileWriter.append(script)
    fileWriter.close
    log.info("Created script file: {}", file.getAbsolutePath)
    context.actorOf(ClusterManager.props(uuid, script, minExecutors), uuid)
    uuid
  }

}
