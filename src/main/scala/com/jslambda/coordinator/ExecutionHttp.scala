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
import com.jslambda.executioner.ScriptExecutioner.ExecutionResult
import spray.json.DefaultJsonProtocol._
import akka.pattern.ask

import scala.util.Success


class ExecutionHttp(coordinator: ActorRef, timeout: Timeout, materializer: ActorMaterializer) extends SprayJsonSupport {
  implicit val requestTimeout: Timeout = timeout
  implicit val actorMaterializer: ActorMaterializer = materializer


  //  implicit def executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val executeExpressionFormat = jsonFormat2(ExecuteExpression)
  implicit val executeExpressionResponse = jsonFormat1(ExecuteExpressionResponse)


  def routes: Route =  executeScriptPath


  def executeScriptPath =
    pathPrefix("execute-expression") {
      post {
        pathEndOrSingleSlash {
          entity(as[ExecuteExpression]) { executeExpression =>

            onComplete(coordinator ? executeExpression) {
              case Success(success) => {
                success match {
                  case ExecutionResult(Some(result)) =>
                    complete(OK, ExecuteExpressionResponse(result))
                  case something =>
                    val a = something
                    println(a)
                    complete(InternalServerError)
                }
              }
              case fail => {
                println(fail)
                complete(InternalServerError)
              }
            }
          }
        }
      }
    }


  //  val ScriptIdSegment = Segment.flatMap(id => Try(id).toOption)

}

case class ExecuteExpression(function: String, params: String)

case class ExecuteExpressionResponse(result: String)
