package com.jslambda.coordinator

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.jslambda.coordinator.CoordinatorActor.CheckStatus

import scala.concurrent.duration.FiniteDuration

object Coordinator {
  def start(system: ActorSystem, log: LoggingAdapter, args: Array[String]) = {


    implicit val as = system
    implicit val materializer = ActorMaterializer()

    val uuid = args.find(arg => arg.startsWith("uuid")) match {
      case Some(uuidParam) =>
        uuidParam.substring("uuid".length + 1)
      case None =>
        log.error("No uuid found while starting coordinator, shutting down!")
        throw new RuntimeException("No uuid found while starting coordinator, shutting down!")
    }
    val httpPort = args.find(arg => arg.startsWith("http-port")) match {
      case Some(httpParam) =>
        httpParam.substring("http-port".length + 1).toInt
      case None =>
        log.error("No http-port found while starting coordinator, shutting down!")
        throw new RuntimeException("No http-port found while starting coordinator, shutting down!")
    }

    log.info("Starting coordinator with uuid: {}", uuid)

    val coordinatorActor = system.actorOf(CoordinatorActor.props("coordinator-actor", uuid), "coordinator-actor")

    val interval = FiniteDuration(5, java.util.concurrent.TimeUnit.SECONDS)

    val blockingDispatcher = system.dispatchers.lookup("blocking-io-dispatcher")

    val api = new ExecutionHttp(coordinatorActor, interval, blockingDispatcher)

    implicit val ec = system.dispatcher
    // status check
    system.scheduler.schedule(interval, interval, coordinatorActor, CheckStatus())

    //    val api = new ExecutionHttp(coordinatorActor, interval, materializer)

    //    val bindingFuture: Future[ServerBinding] =
    Http().bindAndHandle(api.routes, "localhost", httpPort)
    log.info("Starting coordinator on port: {}", httpPort)

  }

}
