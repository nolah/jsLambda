package com.jslambda

import akka.event.Logging
import com.jslambda.coordinator.Coordinator
import com.jslambda.manager.Manager
import com.jslambda.executioner.Executioner
import com.typesafe.config.ConfigFactory


object Main extends App {


  val nodeType = args.find(arg => arg.startsWith("node-type")) match {
    case Some(x) => x.substring("node-type".length + 1)
    case None => "manager"
  }

  val port = args.find(arg => arg.startsWith("-DPORT")) match {
    case Some(x) => x.substring("-DPORT".length + 1).toInt
    case None => 2400
  }

  val configFileName = nodeType + ".conf"

  import akka.actor.ActorSystem

  val myConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port")
  val regularConfig = ConfigFactory.load(configFileName)
  val combined = myConfig.withFallback(regularConfig)
  val complete = ConfigFactory.load(combined)

  implicit val system: ActorSystem = ActorSystem("js-lambda", complete)
  val log = Logging(system, "Main")
  log.info("Starting node: {}.", nodeType)

  val customConfig = system.settings.config.getConfig("custom")

  nodeType match {
    case "manager" =>
      Manager.start(system, log, customConfig.getString("storage-dir"))
    case "coordinator" =>
      Coordinator.start(system, log, args)
    case "executioner" =>
      Executioner.start(system, log, args, customConfig.getString("storage-dir"))
  }

}


