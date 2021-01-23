package org.iv

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.iv.WordCount.getClass
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * This example shows an implementation of WordCount with data from a text socket.
 * To run the example make sure that the service providing the text data is already up and running.
 *
 * To start an example socket text stream on your local machine run netcat from a command line,
 * where the parameter specifies the port number:
 *
 * {{{
 *   nc -lk 9999
 * }}}
 *
 * Usage:
 * {{{
 *   SocketTextStreamWordCount <hostname> <port> <output path>
 * }}}
 *
 * This example shows how to:
 *
 *   - use StreamExecutionEnvironment.socketTextStream
 *   - write a simple Flink Streaming program in scala.
 *   - write and use user-defined functions.
 */
object SocketTextStreamWordCount {
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    implicit val ord = Ordering.fromLessThan[(String, Int)]({ case (f, s) => if (f._2 != s._2) f._2 > s._2 else f._1 < s._1 })
    if (args.length != 2) {
      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream(hostName, port)
    val counts = text.flatMap(_.toLowerCase.split("\\W+") filter (_.nonEmpty))
      .map {
        (_, 1)
      }
      .keyBy(_._1)
      .sum(1)
      .timeWindowAll(Time.of(1,TimeUnit.MINUTES),Time.of(5,TimeUnit.SECONDS) )
      .aggregate(new AggregateFunction[(String, Int), mutable.TreeSet[(String, Int)], List[(String, Int)]] {
        def createAccumulator(): mutable.TreeSet[(String, Int)] = mutable.TreeSet.empty
        def add(value: (String, Int), accumulator: mutable.TreeSet[(String, Int)]): mutable.TreeSet[(String, Int)] = {
          accumulator.filter(_._1!=value._1) += value
        }
        def getResult(accumulator: mutable.TreeSet[(String, Int)]): List[(String, Int)] = {

          val r = accumulator.take(5).toList
//          logger.info("lst = {}",r)
          r
        }
        def merge(a: mutable.TreeSet[(String, Int)], b: mutable.TreeSet[(String, Int)]): mutable.TreeSet[(String, Int)] = a ++ b
      })


    counts print

    env.execute("Scala SocketTextStreamWordCount Example")
  }

}
