package com.jslambda

import java.util.{Date, UUID}

import akka.actor.{Actor, ActorRef, Props}
import com.jslambda.Simulator.JobDone

object Simulator {
  def props(name: String, sourceSupervisorName: String) = Props(new Simulator(name, sourceSupervisorName))

  def name(name: String) = name

  case class JobDone()

}

class Simulator(name: String, sourceSupervisorName: String) extends Actor {

  var sourceName: String = _

  var jobsDone = 0
  var start: Date = _

  import SourceSupervisor._

  val sourceSupervisor: ActorRef = context.actorOf(SourceSupervisor.props(sourceSupervisorName), sourceSupervisorName)

  //  sourceSupervisor ! AddSource(
  //    s"""
  //       |function bubbleSort(a) {
  //       |    var swapped;
  //       |    do {
  //       |        swapped = false;
  //       |        for (var i=0; i < a.length-1; i++) {
  //       |            if (a[i] > a[i+1]) {
  //       |                var temp = a[i];
  //       |                a[i] = a[i+1];
  //       |                a[i+1] = temp;
  //       |                swapped = true;
  //       |            }
  //       |        }
  //       |    } while (swapped);
  //       |    return JSON.stringify(a);;
  //       |}
  //     """.stripMargin)
  sourceSupervisor ! AddSource(
    s"""
       |var array_length;
       |function heap_root(input, i) {
       |    var left = 2 * i + 1;
       |    var right = 2 * i + 2;
       |    var max = i;
       |
       |    if (left < array_length && input[left] > input[max]) {
       |        max = left;
       |    }
       |
       |    if (right < array_length && input[right] > input[max])     {
       |        max = right;
       |    }
       |
       |    if (max != i) {
       |        swap(input, i, max);
       |        heap_root(input, max);
       |    }
       |}
       |
       |function swap(input, index_A, index_B) {
       |    var temp = input[index_A];
       |
       |    input[index_A] = input[index_B];
       |    input[index_B] = temp;
       |}
       |
       |function heapSort(input) {
       |
       |    array_length = input.length;
       |
       |    for (var i = Math.floor(array_length / 2); i >= 0; i -= 1)      {
       |        heap_root(input, i);
       |      }
       |
       |    for (i = input.length - 1; i > 0; i--) {
       |        swap(input, 0, i);
       |        array_length--;
       |
       |
       |        heap_root(input, 0);
       |    }
       |}
     """.stripMargin)

  override def receive: Receive = {
    case message: SourceAdded => {
      sourceName = message.name
      println(s"Source Added: $sourceName")

      start = new Date()

      for (i <- 0 to 100) {
        sourceSupervisor ! ExecuteExpression(sourceName, s"heapSort(${createRandomArray(5000)});")
//        sourceSupervisor ! ExecuteExpression(sourceName, s"bubbleSort(${createRandomArray(5000)});")
      }
    }

    case _: JobDone => {
      jobsDone += 1
      if (jobsDone >= 101) {
        println(s"Total: ${(new Date().getTime - start.getTime)}ms")
      }
    }


  }


  def createRandomArray(arrayLength: Int): String = {
    s"[${(0 to arrayLength).map(_ => (Math.random() * 1000).toInt).mkString(",")}]"
  }


}
