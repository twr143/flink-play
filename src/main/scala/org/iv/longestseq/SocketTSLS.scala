package org.iv.longestseq

/**
 * Created by twr143 on 01.02.2021 at 11:37.
 */

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import org.iv.integrations.postgres.Sinks._
import org.slf4j.LoggerFactory

object SocketTSLS {
  implicit val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val regex = "\\d+"
    if (args.length != 3) {
      System.err.println("USAGE:\nSocketTextStreamLongestSeq <hostname> <port> <source>")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt
    val source = args(2)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1).addSink(cleanLogTableSink)

    val counts = (if (source == "socket")
      env.socketTextStream(hostName, port).flatMap(_.toLowerCase.split("\\W+")
        filter (w => w.nonEmpty && w.matches(regex))).map(_.toInt)
    else if (source == "arrayfill")
      env.fromCollection(List.iterate(0, 100)(a => (a + 1) % 11))
        .map(a => {
          TimeUnit.MILLISECONDS.sleep(100);
          a
        })
    else
      env.fromElements(1, -1, 2, 3, 4, 10, 11, -5, -4, -5)

      )
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .timeWindowAll(Time.of(7, TimeUnit.SECONDS), Time.of(2, TimeUnit.SECONDS))
      .aggregate(LongestSeqAggr())
      .keyBy(_ => ())
      .filterWithState[Int]({
        case (v, Some(state)) => (state != v, Some(v))
        case (v, None) => (true, Some(v))
      })


    counts.print
    counts.addSink(logtableSink[Int])

    env.execute("SocketTextStreamLongestSeq")
  }

}
