import java.util.Properties

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.io.serializer.avro.AvroRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.util._

class Package(var source: String, var destination: String, var size: String, var weight: Integer)

object Sizes extends Enumeration {
  val Small, Medium, Large = Value
}

class Produce {

  def run(conf: Configuration) = {

    val applicationProperties = new Properties
    applicationProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getString("kafka.brokers"))
    applicationProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    applicationProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    applicationProperties.put("schema.registry.url", conf.getString("schemaRegistry.url"))

    val schemaRegistry = new CachedSchemaRegistryClient(conf.getString("schemaRegistry.url"), 1000)

    if (!schemaRegistry.getAllSubjects.contains(conf.getString("schemaSubject.subject"))) {
      Console.println("Registering schema")
      val schema = new Schema.Parser().parse(getClass.getResourceAsStream("/avro/packageV1.avsc"))
      schemaRegistry.register(conf.getString("schemaRegistry.subject"), schema)

    }

    val m = schemaRegistry.getLatestSchemaMetadata(conf.getString("schemaRegistry.subject"))
    val schemaId = m.getId
    val schema = schemaRegistry.getById(schemaId)

    val producer = new KafkaProducer[Any, GenericRecord](applicationProperties)

    def toPackage(packageToSend: Package): GenericRecord = {
        val pack = new GenericData().newRecord(new AvroRecord (), schema).asInstanceOf[GenericRecord]
        pack.put("source", packageToSend.source)
        pack.put("destination", packageToSend.destination)
        pack.put("size", packageToSend.size)
        pack.put("weight", packageToSend.weight)
        pack
    }

    def sendPackage(packageToSend: Package): Unit = {
        System.out.println("Sending to Kafka")
        producer.send(new ProducerRecord(conf.getString("kafka.topics"), toPackage(packageToSend)))
        System.out.println("Sent to Kafka")
    }

    while (true) {
        val source = Random.alphanumeric.take(10).mkString
        val destination = Random.alphanumeric.take(10).mkString
        val size = Random.shuffle(Sizes.values.toList).head.toString
        val weight = Random.nextInt(100)
        val packageToSend = new Package(source, destination, size, weight)
        sendPackage(packageToSend)
        Thread.sleep(5000)
    }
  }
}