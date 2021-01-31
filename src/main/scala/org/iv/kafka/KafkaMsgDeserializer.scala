package org.iv.kafka

/**
 * Created by twr143 on 25.01.2021 at 10:59.
 */

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import protomodels.person.Person
import protomodels.person2.Person2
import scalapb.GeneratedMessage

import com.google.protobuf.any.Any
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass

class KafkaMsgDeserializer extends KafkaDeserializationSchema[GeneratedMessage] {
  def isEndOfStream(gm: GeneratedMessage) = false

  @throws[Exception]
  def deserialize(consumerRecord: ConsumerRecord[Array[Byte],Array[Byte]]): GeneratedMessage = {
      val any = Any.parseFrom(consumerRecord.value())
      if (any.is[Person]) any.unpack[Person]
      else if (any.is[Person2]) any.unpack[Person2]
      else Person("unpack failed", -1)
    }

  def getProducedType: TypeInformation[GeneratedMessage] = getForClass(classOf[GeneratedMessage])
}
