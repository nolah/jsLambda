package com.jslambda.manager

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.jslambda.coordinator.RegistrationHttp

import scala.concurrent.duration.FiniteDuration

object Manager {

  def start(system: ActorSystem, log: LoggingAdapter, storageDir: String) = {
    val superClusterManager = system.actorOf(SuperClusterManager.props("super-cluster-manager", storageDir), "super-cluster-manager")

    implicit val as: ActorSystem = system
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val api = new RegistrationHttp(superClusterManager, FiniteDuration(10, java.util.concurrent.TimeUnit.SECONDS), materializer)

    Http().bindAndHandle(api.routes, "localhost", 9000)
  }
}
