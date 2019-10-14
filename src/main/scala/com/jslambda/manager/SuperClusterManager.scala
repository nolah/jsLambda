package com.jslambda.manager

import java.io.{File, FileWriter, FilenameFilter}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.{Scanner, UUID}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.jslambda.manager.SuperClusterManager._

object SuperClusterManager {

  def props(name: String, storageDir: String) = Props(new SuperClusterManager(name, storageDir))

  case class CoordinatorJoined(uuid: String, actorRef: ActorRef)

  case class CoordinatorRecognized(uuid: String, executioners: List[ActorRef])

  case class ExecutionerJoined(uuid: String, actorRef: ActorRef)

  case class ExecutionerRecognized(uuid: String)

  case class RestoreSubcluster(uuid: String, script: String, startingTcpPort: Int, httpPort: Int, minExecutors: Int)

  case class StartSubcluster(uuid: String, script: String, tcpPort: Int, httpPort: Int, minExecutors: Int)

}

class SuperClusterManager(name: String, storageDir: String) extends Actor with ActorLogging {

  val portsFile = new File(storageDir + "ports")

  var subClusterManagers = Map[Int, ActorRef]()

  var (nextTcpPort, nextHttpPort) = readPortsFile(portsFile)

  Cluster(context.system).subscribe(self, classOf[ClusterDomainEvent])

  val nodeProviderRef = context.actorOf(NodeProvider.props(), "node-provider")

  restoreCluster()

  def receive = {
    case state: CurrentClusterState =>
      log.info("CurrentClusterState: {}.", state)

    case message: MemberEvent =>
      log.info("MemberEvent: {}.", message)
      message.member.address.port match {
        case Some(port) =>
          val range: Int = port / 100
          val startingPort: Int = range * 100
          subClusterManagers.get(startingPort) match {
            case Some(subClusterManager) =>
              subClusterManager forward message
            case None =>
              log.error("Received MemberEvent for member that has no subcluster associated with it, port: {}", port)
          }
        case None =>
          log.error("Received MemberEvent for member without a port!")
      }


    case message: RegisterScript =>
      sender forward createClusterManager(message.script, nextTcpPort, nextHttpPort, message.minimumNodes)
      val (nextTcp, nextHttp) = updatePortsFile(portsFile, nextTcpPort, nextHttpPort)
      nextTcpPort = nextTcp
      nextHttpPort = nextHttp

    case message: StartSubcluster =>
      val subCluster = context.actorOf(SubClusterManager.props(message.uuid, message.script, message.tcpPort, message.httpPort, message.minExecutors, nodeProviderRef), message.uuid)
      subClusterManagers += (message.tcpPort -> subCluster)
      log.info("Starting ClusterManager: uuid: {}, tcpPort: {}, httpPort: {}, minExecutors: {}", message.uuid, message.tcpPort, message.httpPort, message.minExecutors)
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
    fileWriter.close()
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

    val subCluster = context.actorOf(SubClusterManager.props(uuid, script, nextTcpPort, nextHttpPort, minExecutors, nodeProviderRef), uuid)
    subClusterManagers += (nextTcpPort -> subCluster)
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

      self ! StartSubcluster(uuid, script, tcpPort, httpPort, minExecutors)

  })


  }

}
