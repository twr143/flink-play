package org.iv.kafka

import java.lang

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import protomodels.person.Person
import scalapb.GeneratedMessage
import com.google.protobuf.any.Any
import protomodels.person2.Person2

/**
 * Created by twr143 on 25.01.2021 at 12:19.
 */
class KafkaMsgSerializer(topic: String) extends KafkaSerializationSchema[GeneratedMessage] {
  def serialize(element: GeneratedMessage, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    element match {
      case p: Person =>
        new ProducerRecord(topic, p.name.getBytes, Any.pack(p).toByteArray)
      case p: Person2 =>
        new ProducerRecord(topic, p.name.getBytes, Any.pack(p).toByteArray)
    }
  }
}
