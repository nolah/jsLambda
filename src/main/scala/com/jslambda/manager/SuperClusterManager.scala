package com.jslambda.manager

import java.io.{File, FileWriter, FilenameFilter}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.{Scanner, UUID}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.{Cluster, MemberStatus}
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import com.jslambda.coordinator.{RegisterScript, ScriptRegistered}
import com.jslambda.manager.SuperClusterManager.{CoordinatorJoined, ExecutionerJoined, NewMemberIdentifyYourself, RestoreSubcluster}

object SuperClusterManager {

  def props(name: String, storageDir: String) = Props(new SuperClusterManager(name, storageDir))

  case class CoordinatorJoined(uuid: String, actorRef: ActorRef)

  case class CoordinatorRecognized(uuid: String)

  case class NewMemberIdentifyYourself(ninja: String)

  case class ExecutionerJoined(uuid: String, actorRef: ActorRef)

  case class ExecutionerRecognized(uuid: String)

  case class RestoreSubcluster(uuid: String, script: String, startingTcpPort: Int, httpPort: Int, minExecutors: Int)

}

class SuperClusterManager(name: String, storageDir: String) extends Actor with ActorLogging {
  private val mediator = DistributedPubSub(context.system).mediator

  val portsFile = new File(storageDir + "ports")

  var (nextTcpPort, nextHttpPort) = readPortsFile(portsFile)

  mediator ! Subscribe("cluster-bus", self)

  Cluster(context.system).subscribe(self, classOf[ClusterDomainEvent])

  val physicalNodeManager = context.actorOf(PhysicalNodeManager.props("physical-node-manager"), "physical-node-manager")

  restoreCluster

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
      sender forward createClusterManager(message.script, nextTcpPort, nextHttpPort, message.minimumNodes)
      val (nextTcp, nextHttp) = updatePortsFile(portsFile, nextTcpPort, nextHttpPort)
      nextTcpPort = nextTcp
      nextHttpPort = nextHttp
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

  def readPortsFile(portsFile: File): (Int, Int) = {
    val scanner = new Scanner(portsFile)
    val nextTcpPort = scanner.nextInt()
    val nextHttpPort = scanner.nextInt()
    (nextTcpPort, nextHttpPort)
  }

  def updatePortsFile(portsFile: File, nextTcpPort: Int, nextHttpPort: Int): (Int, Int) = {
    // empty the file
    Files.newInputStream(Paths.get(portsFile.getPath), StandardOpenOption.TRUNCATE_EXISTING)
    val fileWriter = new FileWriter(portsFile)
    fileWriter.write(nextTcpPort.toString)
    fileWriter.write("\n")
    fileWriter.write(nextHttpPort.toString)
    fileWriter.close
    log.info("Updated ports file: {}", portsFile.getAbsolutePath)
    (nextTcpPort + 100, nextHttpPort + 100)
  }

  def createClusterManager(script: String, nextTcpPort: Int, nextHttpPort: Int, minExecutors: Int): ScriptRegistered = {
    val uuid = UUID.randomUUID().toString
    val scriptFile = new File(storageDir + uuid + ".script")
    val fileWriter = new FileWriter(scriptFile)
    fileWriter.append(s"$nextTcpPort, $nextHttpPort, $minExecutors")
    fileWriter.append("\n")
    fileWriter.append(script)
    fileWriter.close
    log.info("Created script file: {}", scriptFile.getAbsolutePath)

    context.actorOf(ClusterManager.props(uuid, script, nextTcpPort, nextHttpPort, minExecutors), uuid)
    ScriptRegistered(uuid, "localhost:" + nextHttpPort)
  }

  def restoreCluster(): Unit = {
    log.info("Restoring cluster!")
    val storage = new File(storageDir)
    val fileNames = storage.list(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.matches(".*\\.script")
    })

    fileNames.foreach(f => {
      val uuid = f.take(36)
      val scriptFile = new String(Files.readAllBytes(Paths.get(storageDir + f)))
      val indexOfFirstLineBreak = scriptFile.indexOf("\n")
      val statsLine = scriptFile.take(indexOfFirstLineBreak)
      val script = scriptFile.substring(indexOfFirstLineBreak + 1)
      val stats = statsLine.split(",").map(_.trim.toInt)
      val tcpPort = stats(0)
      val httpPort = stats(1)
      val minExecutors = stats(2)

      context.actorOf(ClusterManager.props(uuid, script, tcpPort, httpPort, minExecutors), uuid)
      log.info("Starting ClusterManager: uuid: {}, tcpPort: {}, httpPort: {}, minExecutors: {}", uuid, tcpPort, httpPort, minExecutors)

    })


  }

}
