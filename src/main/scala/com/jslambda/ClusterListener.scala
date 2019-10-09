package com.jslambda

import akka.actor.{Actor, ActorLogging}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}

class ClusterListener extends Actor with ActorLogging {
  private val mediator = DistributedPubSub(context.system).mediator

  mediator ! Subscribe("cluster-bus", self)

  def receive = {
    case s: String =>
      log.info("Got {}", s)
    case SubscribeAck(Subscribe("content", None, `self`)) =>
      log.info("subscribing")
  }
}
