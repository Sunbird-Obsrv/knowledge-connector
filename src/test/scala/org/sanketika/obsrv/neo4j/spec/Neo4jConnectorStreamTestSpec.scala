package org.sanketika.obsrv.neo4j.spec

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.sanketika.obsrv.neo4j.fixtures.EventFixture
import org.sanketika.obsrv.neo4j.task.{Neo4jConnectorConfig, Neo4jConnectorStreamTask}
import org.scalatest.Matchers._
import org.sunbird.obsrv.BaseMetricsReporter
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil, PostgresConnect}
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class Neo4jConnectorStreamTestSpec extends BaseSpecWithDatasetRegistry {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val pConfig = new Neo4jConnectorConfig(config)
  val kafkaConnector = new FlinkKafkaConnector(pConfig)
  val customKafkaConsumerProperties: Map[String, String] = Map[String, String]("auto.offset.reset" -> "earliest", "group.id" -> "test-event-schema-group")
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort = 9093,
      zooKeeperPort = 2183,
      customConsumerProperties = customKafkaConsumerProperties
    )
  implicit val deserializer: StringDeserializer = new StringDeserializer()

  def testConfiguration(): Configuration = {
    val config = new Configuration()
    config.setString("metrics.reporter", "job_metrics_reporter")
    config.setString("metrics.reporter.job_metrics_reporter.class", classOf[BaseMetricsReporter].getName)
    config
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    BaseMetricsReporter.gaugeMetrics.clear()
    EmbeddedKafka.start()(embeddedKafkaConfig)
    prepareTestData()
    createTestTopics()
    EmbeddedKafka.publishStringMessageToKafka("d4-topic", EventFixture.VALID_NEO4J_JSON_EVENT)

    flinkCluster.before()
  }

  private def prepareTestData(): Unit = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    postgresConnect.execute("insert into datasets(id, type, data_schema, router_config, dataset_config, status, created_by, updated_by, created_date, updated_date, tags) values ('d4', 'dataset', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"topic\":\"d2-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\"}', 'Live', 'System', 'System', now(), now(), ARRAY['Tag1','Tag2']);")
    postgresConnect.execute("""insert into dataset_source_config values('sc1', 'd4', 'neo4j', '{"kafkaBrokers":"localhost:9093","topic":"d4-topic"}', 'Live', null, 'System', 'System', now(), now());""")
    postgresConnect.closeConnection()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      "d4-topic", "local.failed", pConfig.kafkaSystemTopic, "ingest", "local.kafka.connector.in"
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "Neo4jConnectorStreamTest" should "validate the neo4j connector job" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(pConfig)
    val task = new Neo4jConnectorStreamTask(pConfig, kafkaConnector)
    task.process(env)
    Future {
      env.execute(pConfig.jobName)
    }

    val ingestEvents = EmbeddedKafka.consumeNumberMessagesFrom[String]("ingest", 2, timeout = 60.seconds)
    validateIngestEvents(ingestEvents)

    pConfig.inputTopic() should be ("local.kafka.connector.in")
    pConfig.inputConsumer() should be ("")
    pConfig.successTag().getId should be ("neo4j-events")
    pConfig.failedEventsOutputTag().getId should be ("failed-events")
  }

  private def validateIngestEvents(ingestEvents: List[String]): Unit = {
    ingestEvents.size should be(2)
    ingestEvents.foreach{event: String => {
      println("event is " + event)
      if (event.contains(""""dataset":"d4"""")) {
        JSONUtil.getJsonType(event) should be("OBJECT")
        val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](event)
        eventMap("dataset").asInstanceOf[String] should be("d4")
        eventMap.contains("syncts") should be(true)
        eventMap.contains("event") should be(true)
      } else {
        JSONUtil.getJsonType(event) should be ("NOT_A_JSON")
        event.contains(""""event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}""") should be(true)
      }
    }}

  }

}