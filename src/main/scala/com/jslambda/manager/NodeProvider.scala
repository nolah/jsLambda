package com.jslambda.manager

import akka.actor.{Actor, ActorLogging, Props}
import com.jslambda.Main
import com.jslambda.manager.NodeProvider.{StartCoordinator, StartExecutioner}
import com.jslambda.manager.SubClusterManager.AddNewExecutioners

object NodeProvider {
  def props() = Props(new NodeProvider())

  case class StartCluster(minExecutors: Int, uuid: String)

  case class StartExecutioner(uuid: String, tcpPort: Int)

  case class StartCoordinator(uuid: String, tcpPort: Int, httpPort: Int)


  class ClusterStarting

}

class NodeProvider() extends Actor with ActorLogging {


  override def receive: Receive = {
    case message: StartExecutioner =>
      startExecutioner(message.uuid, message.tcpPort)
    case message: StartCoordinator =>
      startCoordinator(message.uuid, message.tcpPort, message.httpPort)


  }

  def startExecutioner(uuid: String, port: Int): Unit = {
    Main.main(Array("node-type=executioner", s"uuid=$uuid", s"-DPORT=$port"))
  }

  def startCoordinator(uuid: String, port: Int, httpPort: Int): Unit = {
    Main.main(Array("node-type=coordinator", s"uuid=$uuid", s"-DPORT=$port", s"http-port=$httpPort"))
  }

}


