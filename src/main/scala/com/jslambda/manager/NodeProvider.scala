package com.jslambda.manager

import akka.actor.{Actor, ActorLogging, Props}
import com.jslambda.Main
import com.jslambda.manager.ClusterManager.{AddNewExecutioners}

object NodeProvider {
  def props(name: String, uuid: String, startingTcpPort: Int, httpPort: Int, minExecutioners: Int) = Props(new NodeProvider(name, uuid, startingTcpPort, httpPort, minExecutioners))

  case class StartCluster(minExecutors: Int, uuid: String)

  class ClusterStarting

}

class NodeProvider(name: String, uuid: String, startingTcpPort: Int, httpPort: Int, minExecutioners: Int) extends Actor with ActorLogging {

  var port = startingTcpPort
  log.info("Starting cluster for uuid: {}", uuid)
  port += 1
  Main.main(Array("node-type=coordinator", s"uuid=${uuid}", s"-DPORT=$port", s"http-port=$httpPort"))
  (0 until minExecutioners) foreach (i => {
    port += 1
    Main.main(Array("node-type=executioner", s"uuid=${uuid}", s"-DPORT=$port"))
  })

  override def receive: Receive = {
    case message: AddNewExecutioners =>
      log.info("Adding executioner for uuid: {}", message.uuid)
      (0 until message.additions) foreach (i => {
        port += 1
        Main.main(Array("node-type=executioner", s"uuid=${message.uuid}", s"-DPORT=$port"))
      })

  }

}


