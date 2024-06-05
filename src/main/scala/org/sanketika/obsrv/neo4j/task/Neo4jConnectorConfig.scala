package org.sanketika.obsrv.neo4j.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

class Neo4jConnectorConfig(override val config: Config) extends BaseJobConfig[String](config, "neo4j-connector") {

  private val serialVersionUID = 2905979435603791379L

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val kafkaDefaultInputTopic: String = config.getString("kafka.input.topic")
  val kafkaDefaultOutputTopic: String = config.getString("kafka.output.topic")
  override def inputTopic(): String = kafkaDefaultInputTopic
  override def inputConsumer(): String = ""
  val neo4jProcessSuccessTag: OutputTag[String] = OutputTag[String]("neo4j-events")
  override def successTag(): OutputTag[String] = neo4jProcessSuccessTag

  val connectorVersion: String = config.getString("connector.version")
  override def failedEventsOutputTag(): OutputTag[String] = OutputTag[String]("failed-events")

  val successfulNeo4jTransformedCount = "neo4j-success-count"

}
