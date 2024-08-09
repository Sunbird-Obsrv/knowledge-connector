package org.sunbird.obsrv.connector.spec

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.Matchers._
import org.sunbird.obsrv.connector.fixtures.EventFixture
import org.sunbird.obsrv.connector.source.IConnectorSource
import org.sunbird.obsrv.connector.{BaseFlinkConnectorSpec, SBKnowlgConnectorSource}

import java.util
import scala.collection.JavaConverters._

class SBKnowlgConnectorTestSpec extends BaseFlinkConnectorSpec with Serializable {

  val customKafkaConsumerProperties: Map[String, String] = Map[String, String]("auto.offset.reset" -> "earliest", "group.id" -> "test-connector-group")
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort = 9093,
      zooKeeperPort = 2183,
      customConsumerProperties = customKafkaConsumerProperties
    )

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
    createTestTopics()
    EmbeddedKafka.publishStringMessageToKafka("sb-knowlg-topic", EventFixture.INVALID_JSON)
    EmbeddedKafka.publishStringMessageToKafka("sb-knowlg-topic", EventFixture.VALID_KNOWLG_EVENT)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List("sb-knowlg-topic").foreach(EmbeddedKafka.createCustomTopic(_))
  }

  override def getConnectorName(): String = "SB-Knowlg-Connector"

  override def getConnectorSource(): IConnectorSource = new SBKnowlgConnectorSource()

  override def testFailedEvents(events: util.List[String]): Unit = {
    events.asScala.size should be (1)
    Console.println("events.asScala.head", events.asScala.head)
    events.asScala.head should be ("""{"error":{"error_code":"JSON_FORMAT_ERR","error_msg":"Not a valid json"},"event":"{\"data\":[{\"nodeUniqueId\":\"lex_auth_0133471820488704007\",","connector_ctx":{"connector_id":"sb-knowlg-connector","dataset_id":"d1","connector_instance_id":"c1","connector_type":"source","data_format":"json","entryTopic":"ingest","state":{},"stats":{}}}""")
  }

  override def testSuccessEvents(events: util.List[String]): Unit = {
    events.asScala.size should be (2)
    events.asScala.head should be ("""{"sourceName":"Global Learning & Growth","identifier":"lex_auth_0133471820488704007","sourceShortName":"Global Learning & Growth"}""")
    events.asScala.last should be ("""{"sourceName":"Global Learning & Growth","identifier":"lex_auth_013326639360794624170","sourceShortName":"Global Learning & Growth"}""")
  }

  override def getConnectorConfigFile(): String = "test-config.json"

  override def getSourceConfig(): Map[String, AnyRef] = {
    Map(
      "source_kafka_broker_servers" -> "localhost:9093",
      "source_kafka_consumer_id" -> "knowlg-connector",
      "source_kafka_auto_offset_reset" -> "earliest",
      "source_data_format" -> "json",
      "source_kafka_topic" -> "sb-knowlg-topic",
    )
  }

}