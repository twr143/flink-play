package org.iv.kafka

import java.util.Properties

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, FlinkKafkaProducer011}
import scalapb.GeneratedMessage

/**
 * Created by twr143 on 25.01.2021 at 10:43.
 */
object DatastreamJob {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val topicName = "25jan21"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")
    val consumer  = new FlinkKafkaConsumer[GeneratedMessage](topicName,
            new KafkaMsgDeserializer(), properties)
    val producer  = new FlinkKafkaProducer[GeneratedMessage](topicName,
            new KafkaMsgSerializer(s"$topicName-out"), properties,Semantic.NONE)

    consumer.setStartFromLatest()
    val stream = env.addSource(consumer)
    stream.addSink(producer)
    stream.print()
    env.execute("Flink kafka api")

  }
}
