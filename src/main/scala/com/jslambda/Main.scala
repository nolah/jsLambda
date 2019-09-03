package com.jslambda

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Date

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.jslambda.actor.{ClusterDomainEventListener, Endpoint, ScriptCollection}
import com.typesafe.config.{Config, ConfigFactory}
import org.mozilla.javascript.Context

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.TimeUnit
import akka.pattern._
import org.slf4j.LoggerFactory

object Main extends App {


  val configFileName = args.find(arg => arg.startsWith("config-file-name")) match {
    case Some(x) => x.substring("config-file-name".length + 1)
    case None => "seed.conf"
  }
  val config = ConfigFactory.load(configFileName)
  implicit val system: ActorSystem = ActorSystem("jsLambda", config)
  val log = Logging(system, "Main")
  log.info("Starting actor system with config: {}.", configFileName)

  val customConfig = system.settings.config.getConfig("custom")
  implicit val ec = system.dispatcher //bindAndHandle requires an implicit ExecutionContext
  implicit val materializer = ActorMaterializer()

  val scriptCollection = system.actorOf(ScriptCollection.props("script-collection", customConfig.getString("storage-dir")), "script-collection")

  val api = new Endpoint(scriptCollection, FiniteDuration(10, java.util.concurrent.TimeUnit.SECONDS), materializer)

  system.actorOf(Props(new ClusterDomainEventListener), "cluster-listener")
  val bindingFuture: Future[ServerBinding] =
    Http().bindAndHandle(api.routes, "localhost", 9000)

  log.info("Actor system: {} started.", system.name)

}


