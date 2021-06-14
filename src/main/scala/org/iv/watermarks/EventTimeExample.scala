package org.iv.watermarks

/**
 * Created by twr143 on 14.06.2021 at 15:34.
 */
import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object EventTimeExample {


  case class ExampleType(time: Long, value: Long)
  class MyAssigner extends SerializableTimestampAssigner[ExampleType] {
    def extractTimestamp(element: ExampleType, recordTimestamp: Long): Long = element.time
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
      ExampleType(1525132960000L, 4),
      ExampleType(1525132900000L, 5)
    ))
      .assignTimestampsAndWatermarks(WatermarkStrategy.
        forBoundedOutOfOrderness(Duration.ofSeconds(50))   //if 50 - 5 exists because 960-50 in the window
        // [860,920],if 30 -  5 does not, because 960-30 is in the window [920,980]
        .withTimestampAssigner(new MyAssigner))

    val windows = simple.map{a => Thread.sleep(250); a} //delay to allow watermark to be updated
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