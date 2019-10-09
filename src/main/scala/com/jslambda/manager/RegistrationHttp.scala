package com.jslambda.coordinator

import java.lang.invoke.MethodHandleInfo
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, OK}
import akka.http.scaladsl.server.Directives.{as, complete, entity, pathEndOrSingleSlash, pathPrefix, post}
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import akka.http.scaladsl.server.Directives._
import akka.stream._
import spray.json.DefaultJsonProtocol._
import akka.pattern.ask
//import com.jslambda.executioner.ScriptCollection.{ExecuteScript, ExecutionResult, RegisterScript, ScriptRegistered}

import scala.util.{Failure, Success, Try}


class RegistrationHttp(superClusterManager: ActorRef, timeout: Timeout, materializer: ActorMaterializer) extends SprayJsonSupport {
  implicit val requestTimeout: Timeout = timeout
  implicit val actorMaterializer: ActorMaterializer = materializer


  //  implicit def executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val registerScriptFormat = jsonFormat2(RegisterScript)
  implicit val registerScriptResponseFormat = jsonFormat1(RegisterScriptResponse)
  implicit val executeExpressionResponse = jsonFormat1(ExecuteExpressionResponse)


  //  def routes: Route = registerScript ~ executeScript
  def routes: Route = registerScript

  def registerScript =
    pathPrefix("register-script") {
      post {
        pathEndOrSingleSlash {
          entity(as[RegisterScript]) { registerScript =>
            println(registerScript)
            onComplete(superClusterManager ? registerScript) {
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
//            superClusterManager ! registerScript
//            complete(OK)
          }
        }
      }
    }

//  def executeScriptPath =
//    pathPrefix("execute-expression") {
//      post {
//        pathEndOrSingleSlash {
//          entity(as[ExecuteScript]) { executeScript =>
//            onComplete(scriptCollection ? executeScript) {
//              case Success(success) => {
//                success match {
//                  case ExecutionResult(Some(result), None) =>
//                    complete(OK, ExecuteExpressionResponse(result))
//                  case something =>
//                    val a = something
//                    println(a)
//                    complete(InternalServerError)
//                }
//              }
//              case fail => {
//                println(fail)
//                complete(InternalServerError)
//              }
//            }
//          }
//        }
//      }
//    }


  //  val ScriptIdSegment = Segment.flatMap(id => Try(id).toOption)

}

case class RegisterScript(script: String, minimumNodes: Int)
case class ScriptRegistered(uuid: String)

case class RegisterScriptResponse(uuid: String)
case class ExecuteExpressionResponse(result: String)
