package org.iv.integrations.postgres


import java.util.concurrent.TimeUnit




import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.iv.aggregate._
import org.slf4j.LoggerFactory
import Sinks._

object SocketTSNCPG {
  val logger = LoggerFactory.getLogger(getClass)



  def main(args: Array[String]): Unit = {
    val regex = "\\d+"
    if (args.length != 3) {
      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port> <source>")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt
    val source = args(2)
    val k = 10
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1).addSink(cleanLogTableSink)

    val counts = (if (source == "socket")
      env.socketTextStream(hostName, port).flatMap(_.toLowerCase.split("\\W+")
        filter (w => w.nonEmpty && w.matches(regex))).map(_.toInt) else
      env.fromCollection(List.iterate(0, 100)(a => (a + 1) % 11))
        .map(a => {
          TimeUnit.MILLISECONDS.sleep(100);
          a
        })
      )
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .timeWindowAll(Time.of(1, TimeUnit.MINUTES), Time.of(3, TimeUnit.SECONDS))
      .aggregate(MaxComposedAggr(k))
      .flatMap(_.asInstanceOf[List[(Int, Int)]]).keyBy(_._1)
      .filterWithState[Set[(Int, Int)]]({
        case (pair, Some(state)) => (!state.contains(pair), Some(state + pair))
        case (pair, None) => (true, Some(Set(pair)))
      })


    counts.print
    counts.addSink(logtableSink[(Int,Int)])


    env.execute("Scala SocketTSNCPG Example")
  }

}
