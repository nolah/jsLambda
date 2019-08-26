package com.jslambda.actor

import java.lang.invoke.MethodHandleInfo
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, OK}
import akka.http.scaladsl.server.Directives.{as, complete, entity, pathEndOrSingleSlash, pathPrefix, post}
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.jslambda.EventMarshalling
import akka.http.scaladsl.server.Directives._
import akka.stream._
import spray.json.DefaultJsonProtocol._
import akka.pattern.ask
import com.jslambda.actor.ScriptCollection.{ExecuteScript, ExecutionResult, RegisterScript, ScriptRegistered}

import scala.util.{Failure, Success, Try}


class Endpoint(scriptCollection: ActorRef, timeout: Timeout, materializer: ActorMaterializer) extends EventMarshalling with SprayJsonSupport {
  implicit val requestTimeout: Timeout = timeout
  implicit val actorMaterializer: ActorMaterializer = materializer


  //  implicit def executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val registerScriptFormat = jsonFormat1(RegisterScript)
  implicit val executeScriptFormat = jsonFormat2(ExecuteScript)
  implicit val registerScriptResponseFormat = jsonFormat1(RegisterScriptResponse)
  implicit val executeExpressionResponse = jsonFormat1(ExecuteExpressionResponse)


  //  def routes: Route = registerScript ~ executeScript
  def routes: Route = registerScript ~ executeScriptPath

  def registerScript =
    pathPrefix("register-script") {
      post {
        pathEndOrSingleSlash {
          entity(as[RegisterScript]) { registerScript =>
            println(registerScript)
            onComplete(scriptCollection ? registerScript) {
              case Success(success) => {
                success match {
                  case ScriptRegistered(uuid) =>
                    complete(OK, RegisterScriptResponse(uuid))
                  case _ =>
                    complete(InternalServerError)
                }
              }
              case _ => {
                complete(InternalServerError)
              }
            }

          }
        }
      }
    }

  def executeScriptPath =
    pathPrefix("execute-expression") {
      post {
        pathEndOrSingleSlash {
          entity(as[ExecuteScript]) { executeScript =>
            onComplete(scriptCollection ? executeScript) {
              case Success(success) => {
                success match {
                  case ExecutionResult(Some(result), None) =>
                    complete(OK, ExecuteExpressionResponse(result))
                  case something =>
                    val a = something
                    println(a)
                    complete(InternalServerError)
                }
              }
              case _ => {
                complete(InternalServerError)
              }
            }
          }
        }
      }
    }


  //  val ScriptIdSegment = Segment.flatMap(id => Try(id).toOption)

}

case class RegisterScriptResponse(uuid: String)
case class ExecuteExpressionResponse(result: String)
