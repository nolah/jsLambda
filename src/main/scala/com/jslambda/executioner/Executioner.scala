package com.jslambda.executioner

import java.io.{File, FileReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.event.LoggingAdapter

object Executioner {
  def start(system: ActorSystem, log: LoggingAdapter, args: Array[String], storageDir: String) = {

    val uuid = args.find(arg => arg.startsWith("uuid")) match {
      case Some(uuidParam) =>
        uuidParam.substring("uuid".length + 1)
      case None =>
        log.error("No uuid found while starting executioner, shutting down!")
        throw new RuntimeException("No uuid found while starting executioner, shutting down!")
    }

    log.info("Starting executioner with uuid: {}", uuid)

    val script = new String(Files.readAllBytes(Paths.get(storageDir + uuid + ".script")))
    system.actorOf(ScriptExecutioner.props(uuid, script), "executioner-actor")

  }

}
