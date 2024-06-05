package org.sanketika.obsrv.neo4j.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sanketika.obsrv.neo4j.task.Neo4jConnectorConfig
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable.{Map => MMap}

class Neo4jEventProcessFunction(config: Neo4jConnectorConfig)
                               (implicit val eventTypeInfo: TypeInformation[String]) extends BaseProcessFunction[String, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[Neo4jEventProcessFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.successfulNeo4jTransformedCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }

  override def processElement(event: String,
                              context: ProcessFunction[String, String]#Context,
                              metrics: Metrics): Unit = {
    val neo4jEvent = JSONUtil.deserialize[Map[String, AnyRef]](event)
    val data = neo4jEvent.getOrElse("data", List.empty).asInstanceOf[List[Map[String, AnyRef]]]
    data.foreach { transaction => {
      val nodeUniqueId = transaction.getOrElse("nodeUniqueId", "").asInstanceOf[String]
      val transactionData = transaction.getOrElse("transactionData", Map.empty).asInstanceOf[Map[String, AnyRef]]
      val properties = transactionData.getOrElse("properties", Map.empty).asInstanceOf[Map[String, AnyRef]]
      val nodeData = MMap[String, AnyRef]()
      properties.foreach {
        case (key, value) => nodeData.put(key, value.asInstanceOf[Map[String, AnyRef]].getOrElse("newValue", null))
      }
      nodeData.put("identifier", nodeUniqueId)
      context.output(config.successTag(), JSONUtil.serialize(nodeData))
    }
    }
  }

}
