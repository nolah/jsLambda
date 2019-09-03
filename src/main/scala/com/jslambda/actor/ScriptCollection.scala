package com.jslambda.actor

import java.io.{File, FileReader, FileWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.UUID

import scala.collection.JavaConverters._
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Props}
import com.jslambda.actor.ScriptCollection.{ExecuteScript, ExecutionResult, RegisterScript, ScriptRegistered}

object ScriptCollection {
  def props(name: String, storageDir: String) = Props(new ScriptCollection(name, storageDir))

  case class ExecuteScript(uuid: String, function: String, parameters: String)

  case class ExecutionResult(result: Option[String], error: Option[String])


  case class RegisterScript(script: String)

  case class ScriptRegistered(id: String)


  case class ExecuteExpression(expression: String)

  case class ExpressionExecuted(result: String)


}

class ScriptCollection(name: String, storageDirPath: String) extends Actor with ActorLogging {
  val storageDir = new File(storageDirPath)
  log.info("Absolute path of storage dir: {}.", storageDir.getAbsolutePath)
  loadFromFile(storageDir)


  override def receive: Receive = {
    case RegisterScript(script) =>
      val uuid = addScript(script)
      sender forward ScriptRegistered(uuid)
    case message: ExecuteScript =>
      context.child(message.uuid) match {
        case None =>
          sender forward ExecutionResult(None, Some("NotFound"))
        case Some(script) =>
          script forward message
      }
  }

  def loadFromFile(storageDir: File) = {
    log.info("Loading scripts from dir: {}.", storageDirPath)
    val files = storageDir.listFiles()
    files.foreach(file => {
      if (file.getName.endsWith(".script")) {
        log.info("Reading script from file: {}.", file.getName)
        val script = Files.readAllLines(Paths.get(file.getPath), StandardCharsets.UTF_8).asScala.mkString("\n")
        val uuid = getUuidFromName(file.getName)
        log.info("Loaded script with uuid: {}.", uuid)
        createScriptActor(uuid, script)
      }
    })
  }

  def addScript(script: String): String = {
    val uuid = UUID.randomUUID().toString
    createScriptActor(uuid, script)
    val fileName = uuid + ".script"
    log.info("Storing script to file: {}.", fileName)
    val scriptFile = new File(storageDir, fileName)
    val fw = new FileWriter(scriptFile).append(script)
    fw.close()
    log.info("Successfully stored script to file: {}.", scriptFile.getName)
    uuid
  }

  def createScriptActor(uuid: String, script: String): ActorRef = {
    log.info("Creating script actor with uuid: {}.", uuid)
    context.actorOf(Script.props(uuid, script), uuid)
  }

  def getUuidFromName(fileName: String): String = {
    log.info("Getting uuid from fileName: {}.", fileName)
    fileName.split("\\.")(0)
  }


}
