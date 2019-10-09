package com.jslambda

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Date

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import com.jslambda.coordinator.{Coordinator, CoordinatorActor}
import com.jslambda.manager.Manager
//import com.jslambda.coordinator.{Coordinator, Endpoint}
import com.jslambda.executioner.Executioner
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

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
  //  implicit val ec = system.dispatcher
  //  implicit val materializer = ActorMaterializer()

  nodeType match {
    case "manager" =>
      Manager.start(system, log, customConfig.getString("storage-dir"))
    //    case "registration" =>
    //      Registration.start
    case "coordinator" =>
      Coordinator.start(system, log, args)
    case "executioner" =>
      Executioner.start(system, log, args, customConfig.getString("storage-dir"))
  }

  //  val scriptCollection = system.actorOf(ScriptCollection.props("script-collection", customConfig.getString("storage-dir")), "script-collection")
  //
  //  val api = new Endpoint(scriptCollection, FiniteDuration(10, java.util.concurrent.TimeUnit.SECONDS), materializer)
  //
  //  system.actorOf(Props(new ClusterDomainEventListener), "cluster-listener")
  //  val bindingFuture: Future[ServerBinding] =
  //    Http().bindAndHandle(api.routes, "localhost", 9000)
  //
  //  log.info("Actor system: {} started.", system.name)
//  logCalcs(1, 50, 2)
//  logCalcs(1, 50, 10)
//  logCalcs(1, 50, 100)
//  logCalcs(3, 50, 100)
//  logCalcs(5, 50, 100)
//  logCalcs(5, 50, 401)
//  logCalcs(5, 50, 279)
//  logCalcs(4, 50, 280)
//  logCalcs(4, 50, 319)
//  logCalcs(5, 50, 319)
//  logCalcs(5, 50, 279)


  def logCalcs(nodes: Int, avgExec: Int, enqueued: Int): Unit = {
    log.info("Current nodes: {}, avgExec: {}, enqueued: {}, preferredNodes: {}", nodes, avgExec, enqueued, adjustClusterSize(nodes, avgExec, enqueued))

  }


  def adjustClusterSize(nodes: Int, avgExecutionTime: Int, enqueued: Int): Int = {
    if (avgExecutionTime == 0) {
      return nodes
    }

    if (5000 / avgExecutionTime * 0.8 * nodes < enqueued) {
      Math.ceil(nodes * (1 + CoordinatorActor.rampUpRate)).toInt
    } else if (5000 / avgExecutionTime * nodes * 0.8 * 0.7 > enqueued) {
      Math.floor(nodes * (1 - CoordinatorActor.backOffRate)).toInt
    } else {
      nodes
    }

  }

}


