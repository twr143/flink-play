package org.iv.watermarks

import org.apache.flink.api.scala._

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows


/**
 * Created by twr143 on 14.06.2021 at 16:10.
 */
object EventTimeExample2 {
  case class ExampleType(time: Long, value: Long)
  class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[ExampleType] {
    override def extractTimestamp(element: ExampleType, previousElementTimestamp: Long): Long = {
      element.time
    }

    override def checkAndGetNextWatermark(lastElement: ExampleType, extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - 90000)
    }
  }
  def main(args: Array[String]) {

    // Set up environment
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Example S3 path
    val simple = env.fromCollection(Seq(
      ExampleType(1525132800000L, 1),
      ExampleType(1525132800000L, 2) ,
      ExampleType(1525132940000L, 3),
      ExampleType(1525132800000L, 4)
    ))
      .assignTimestampsAndWatermarks(new PunctuatedAssigner)

    val windows = simple
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply{
       (window, iter, collector: Collector[(Long, Long, String)]) => {
        collector.collect(window.getStart, window.getEnd, iter.map(_.value).toString())
      }
    }

    windows.print
    env.execute("TimeStampExample")
  }

}
