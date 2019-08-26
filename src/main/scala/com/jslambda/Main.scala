package com.jslambda

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Date

import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.jslambda.actor.{Endpoint, ScriptCollection}
import com.typesafe.config.{Config, ConfigFactory}
import org.mozilla.javascript.Context

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.TimeUnit

object Main extends App {

  //  val config = ConfigFactory.load("singlenode")
  val config1 = ConfigFactory.load("resources/application.properties")
  val config2 = ConfigFactory.load("application.properties")
  implicit val system: ActorSystem = ActorSystem("jsLambda", config2)

  implicit val ec = system.dispatcher //bindAndHandle requires an implicit ExecutionContext
  implicit val materializer = ActorMaterializer()

  val scriptCollection = system.actorOf(ScriptCollection.props("script-collection"), "script-collection")
  //  val scriptCollection = system.actorOf(ScriptCollection.props("script-collection"), "script-collection")

  val api = new Endpoint(scriptCollection, FiniteDuration(1, java.util.concurrent.TimeUnit.SECONDS), materializer)

  val bindingFuture: Future[ServerBinding] =
    Http().bindAndHandle(api.routes, "localhost", 9000)
  //
  //  //#create-actors
  //  // Create the printer actor
  //  val simulator:ActorRef = system.actorOf(Simulator.props("simulator", "SourceSupervisor"), "simulator")


}


