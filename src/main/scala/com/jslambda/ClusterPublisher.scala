package com.jslambda

import akka.actor.Actor
import akka.cluster.pubsub.DistributedPubSub

class ClusterPublisher extends Actor {

  import akka.cluster.pubsub.DistributedPubSubMediator.Publish

  private val mediator = DistributedPubSub(context.system).mediator

  def receive = {
    case in: String =>
      val out = in.toUpperCase
      mediator ! Publish("content", out)
  }
}
