package org.iv.keyselectors

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

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory


object SocketTSKS4 {
  val logger = LoggerFactory.getLogger(getClass)

  val config = OutputFileConfig
    .builder()
    .withPartPrefix("prefix")
    .withPartSuffix(".ext")
    .build()

  def sink(outputPath: String): StreamingFileSink[List[(String, Int)]] = StreamingFileSink
    .forRowFormat(new Path(outputPath), new SimpleStringEncoder[List[(String, Int)]]("UTF-8"))
    .withOutputFileConfig(config)
    //    .withRollingPolicy(
    //      DefaultRollingPolicy.builder()
    //        .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
    //        .withInactivityInterval(TimeUnit.MINUTES.toMillis(2))
    //        .withMaxPartSize(1024 * 1024 * 1024)
    //        .build())
    .build()


  def main(args: Array[String]): Unit = {
    val regex = "\\d+"
    if (args.length != 2) {
      System.err.println("USAGE:\nSocketTSKS4 <hostname> <port>")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setMaxParallelism(4)

    val text = env.socketTextStream(hostName, port)
    val counts = text.flatMap(_.toLowerCase.split("\\W+") filter (w => w.nonEmpty && w.matches(regex)))
      .map(_.toInt).map(a => (Value(a), 1))
      .keyBy(_._1.v)
      .sum(1)
      .map(pair => (pair._1, pair._1.v % 2))
      .keyBy(_._2)
      .timeWindowAll(Time.of(1, TimeUnit.MINUTES), Time.of(3, TimeUnit.SECONDS))
      .aggregate(CustomAggr())

    counts.print

    env.execute("Scala SocketTSKS4 Example")
  }

}
