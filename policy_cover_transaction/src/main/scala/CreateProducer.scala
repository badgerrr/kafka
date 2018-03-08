package com.jack.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.util.Random


class CreateProducer(producerTopic: String, kafkaBroker: String, sourceFile: String) {

  val JsonValues: Iterator[String] = Source.fromFile(sourceFile).getLines()

  val topic: String = producerTopic
  val brokers: String = kafkaBroker
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", producerTopic)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  for (line <- Source.fromFile(sourceFile).getLines())
    producer.send(new ProducerRecord[String, String](topic, "192.168.2." + rnd.nextInt(255), line))

  producer.close()
}
