package com.jslambda.manager

import akka.actor.{ActorSystem, Props}
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import com.jslambda.coordinator.RegistrationHttp

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object Manager {

  def start(system: ActorSystem, log: LoggingAdapter, storageDir: String) = {
//    system.actorOf(Props(new ClusterDomainEventListener), "cluster-listener")
    val superClusterManager = system.actorOf(SuperClusterManager.props("super-cluster-manager", storageDir), "super-cluster-manager")

    implicit val as = system
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

    val api = new RegistrationHttp(superClusterManager, FiniteDuration(10, java.util.concurrent.TimeUnit.SECONDS), materializer)

    //    val bindingFuture: Future[ServerBinding] =
    Http().bindAndHandle(api.routes, "localhost", 9000)
  }

}
