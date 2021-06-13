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

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.iv.aggregate._
import org.slf4j.LoggerFactory




object SocketTSNC3 {
  val logger = LoggerFactory.getLogger(getClass)

  val config = OutputFileConfig
    .builder()
    .withPartPrefix("prefix")
    .withPartSuffix(".ext")
    .build()

  def sink2(outputPath: String): StreamingFileSink[List[(Int, Int)]] = StreamingFileSink
    .forRowFormat(new Path(outputPath), new SimpleStringEncoder[List[(Int, Int)]]("UTF-8"))
    .withOutputFileConfig(config)
    //    .withRollingPolicy(
    //      DefaultRollingPolicy.builder()
    //        .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
    //        .withInactivityInterval(TimeUnit.MINUTES.toMillis(2))
    //        .withMaxPartSize(1024 * 1024 * 1024)
    //        .build())
    .build()

  def main(args: Array[String]): Unit = {
    val regex = "[\\d-]+"
    if (args.length != 3) {
      System.err.println(s"USAGE:\nSocketTextStreamWordCount <hostname> <port> <source> args len ${args.mkString(" ")}")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt
    val source = args(2)
    val k = 10
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    RestartStrategies.failureRateRestart(1, org.apache.flink.api.common.time.Time.seconds(1), org.apache.flink.api.common.time.Time.seconds(1))
//    env.setParallelism(2)
//    env.setMaxParallelism(4)
    val counts = (if (source == "socket")
      env.socketTextStream(hostName, port).flatMap(_.toLowerCase.split("\\W-")
        filter (w => w.nonEmpty && w.matches(regex))).map(_.toInt) else
      env.fromCollection(List.iterate(0, 100)(a => (a + 1) % 11))
        .map(a => {
          TimeUnit.MILLISECONDS.sleep(100); a
        })
      )
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .timeWindowAll(Time.of(5, TimeUnit.SECONDS), Time.of(3, TimeUnit.SECONDS))
      .aggregate(MaxComposedAggr(k))
      .flatMap(_.asInstanceOf[List[(Int, Int)]]).keyBy(_._1)
      .filterWithState[Set[(Int, Int)]]({
        case (pair, Some(state)) => (!state.contains(pair), Some(state + pair))
        case (pair, None) => (true, Some(Set(pair)))
      })


    counts.print
    logger.info("c result {}", counts)
    counts.addSink(sink2("sout").asInstanceOf[SinkFunction[(Int,Int)]])

    env.execute("Scala SocketTSNC3 Example")
  }

}
