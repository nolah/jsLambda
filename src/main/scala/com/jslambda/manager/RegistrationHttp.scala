package com.jslambda.manager

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, OK}
import akka.http.scaladsl.server.Directives.{as, complete, entity, pathEndOrSingleSlash, pathPrefix, post}
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import akka.http.scaladsl.server.Directives._
import akka.stream._
import spray.json.DefaultJsonProtocol._
import akka.pattern.ask

import scala.util.Success

class RegistrationHttp(superClusterManager: ActorRef, timeout: Timeout, materializer: ActorMaterializer) extends SprayJsonSupport {
  implicit val requestTimeout: Timeout = timeout
  implicit val actorMaterializer: ActorMaterializer = materializer

  implicit val registerScriptFormat = jsonFormat2(RegisterScript)
  implicit val registerScriptResponseFormat = jsonFormat2(RegisterScriptResponse)
  implicit val executeExpressionResponse = jsonFormat1(ExecuteExpressionResponse)

  def routes: Route = registerScript

  def registerScript =
    pathPrefix("register-script") {
      post {
        pathEndOrSingleSlash {
          entity(as[RegisterScript]) { registerScript =>
            onComplete(superClusterManager ? registerScript) {
              case Success(success) => {
                success match {
                  case ScriptRegistered(uuid, url) =>
                    complete(OK, RegisterScriptResponse(uuid, url))
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
}

case class RegisterScript(script: String, minimumNodes: Int)

case class ScriptRegistered(uuid: String, url: String)

case class RegisterScriptResponse(uuid: String, url: String)

case class ExecuteExpressionResponse(result: String)
