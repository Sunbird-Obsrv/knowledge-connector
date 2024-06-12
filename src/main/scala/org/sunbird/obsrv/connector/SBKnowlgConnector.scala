package org.sunbird.obsrv.connector

import com.typesafe.config.Config
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetResetStrategy}
import org.sunbird.obsrv.connector.model.Models
import org.sunbird.obsrv.connector.source.{IConnectorSource, SourceConnector, SourceConnectorFunction}
import org.sunbird.obsrv.job.exception.UnsupportedDataFormatException

import java.nio.charset.StandardCharsets
import java.util.Properties

object SBKnowlgConnector {
  def main(args: Array[String]): Unit = {
    SourceConnector.process(args, new SBKnowlgConnectorSource)
  }
}
class SBKnowlgConnectorSource extends IConnectorSource {

  private def kafkaConsumerProperties(config: Config): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", config.getString("source_kafka_broker_servers"))
    properties.setProperty("group.id", config.getString("source_kafka_consumer_id"))
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    properties.setProperty("auto.offset.reset", config.getString("source_kafka_auto_offset_reset"))
    properties
  }

  private def kafkaSource(config: Config): KafkaSource[String] = {
    val dataFormat = config.getString("source_data_format")
    if (!"json".equals(config.getString("source_data_format"))) {
      throw new UnsupportedDataFormatException(dataFormat)
    }
    KafkaSource.builder[String]()
      .setTopics(config.getString("source_kafka_topic"))
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(kafkaConsumerProperties(config))
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .build()
  }
  override def getSourceStream(env: StreamExecutionEnvironment, config: Config): SingleOutputStreamOperator[String] = {
    env.fromSource(kafkaSource(config), WatermarkStrategy.noWatermarks[String](), config.getString("source_kafka_consumer_id")).uid(config.getString("source_kafka_consumer_id"))
  }

  override def getSourceFunction(contexts: List[Models.ConnectorContext]): SourceConnectorFunction = {
    new SBKnowlgConnectorFunction(contexts)
  }
}

class StringDeserializationSchema extends KafkaRecordDeserializationSchema[String] {
  private val serialVersionUID = -3224825136576915426L

  override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[String]): Unit = {
    out.collect(new String(record.value(), StandardCharsets.UTF_8))
  }
}