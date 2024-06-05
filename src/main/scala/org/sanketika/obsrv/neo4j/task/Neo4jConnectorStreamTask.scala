package org.sanketika.obsrv.neo4j.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.joda.time.{DateTime, DateTimeZone}
import org.sanketika.obsrv.neo4j.functions.Neo4jEventProcessFunction
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.model.{DatasetModels, DatasetStatus}

import java.io.File

class Neo4jConnectorStreamTask(config: Neo4jConnectorConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[String] {

  private val serialVersionUID = -7729362727131516112L
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    process(env)
    env.execute(config.jobName)
  }

  override def processStream(dataStream: DataStream[String]): DataStream[String] = {
    null
  }
  // $COVERAGE-ON$
  def process(env: StreamExecutionEnvironment): Unit = {
    val datasetSourceConfig: Option[List[DatasetModels.DatasetSourceConfig]] = DatasetRegistry.getAllDatasetSourceConfig()
    datasetSourceConfig.map { configList =>
      configList.filter(config => {
        config.connectorType.equalsIgnoreCase("neo4j") && config.status.equalsIgnoreCase(DatasetStatus.Live.toString)
      }).map { dataSourceConfig =>
          val dataStream: DataStream[String] = getStringDataStream(
              env=env,
              config=config,
              kafkaTopics = List(dataSourceConfig.connectorConfig.topic),
              kafkaConsumerProperties = config.kafkaConsumerProperties(kafkaBrokerServers = Some(dataSourceConfig.connectorConfig.kafkaBrokers),
              kafkaConsumerGroup = Some(s"neo4j-${dataSourceConfig.connectorConfig.topic}-consumer")),
              consumerSourceName = s"neo4j-${dataSourceConfig.datasetId}-${dataSourceConfig.connectorConfig.topic}", kafkaConnector
            )
          val datasetId = dataSourceConfig.datasetId
          val kafkaOutputTopic = DatasetRegistry.getDataset(datasetId).get.datasetConfig.entryTopic
          val resultStream: DataStream[String] = dataStream.process(new Neo4jEventProcessFunction(config))
            .getSideOutput(config.successTag()).map { event =>
              val syncts = java.lang.Long.valueOf(new DateTime(DateTimeZone.UTC).getMillis)
              s"""{"dataset":"$datasetId","syncts":$syncts,"event":$event}"""
            }
            resultStream.sinkTo(kafkaConnector.kafkaSink[String](kafkaOutputTopic))
            .name(s"$datasetId-neo4j-connector-sink").uid(s"$datasetId-neo4j-connector-sink")
            .setParallelism(config.downstreamOperatorsParallelism)
      }.orElse(List(addDefaultOperator(env, config, kafkaConnector)))
    }.orElse(Some(addDefaultOperator(env, config, kafkaConnector)))
  }

  def addDefaultOperator(env: StreamExecutionEnvironment, config: Neo4jConnectorConfig, kafkaConnector: FlinkKafkaConnector): DataStreamSink[String] = {
    println("default operator invoked..")
    val dataStreamSink: DataStreamSink[String] = getStringDataStream(env, config, kafkaConnector)
      .sinkTo(kafkaConnector.kafkaSink[String](config.kafkaDefaultOutputTopic))
      .name(s"neo4j-connector-default-sink").uid(s"neo4j-connector-default-sink")
      .setParallelism(config.downstreamOperatorsParallelism)
    dataStreamSink
  }

}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object Neo4jConnectorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("neo4j-connector.conf").withFallback(ConfigFactory.systemEnvironment()))
    val connectorConfig = new Neo4jConnectorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(connectorConfig)
    val task = new Neo4jConnectorStreamTask(connectorConfig, kafkaUtil)
    task.process()
  }

}
// $COVERAGE-ON$