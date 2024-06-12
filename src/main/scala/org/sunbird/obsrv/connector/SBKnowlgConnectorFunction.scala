package org.sunbird.obsrv.connector

import org.json.{JSONException, JSONObject}
import org.sunbird.obsrv.connector.model.Models
import org.sunbird.obsrv.connector.source.SourceConnectorFunction
import org.sunbird.obsrv.job.model.Models.ErrorData
import org.sunbird.obsrv.job.util.JSONUtil
import scala.collection.mutable

class SBKnowlgConnectorFunction(contexts: List[Models.ConnectorContext]) extends SourceConnectorFunction(contexts) {
  override def processEvent(event: String, onSuccess: String => Unit, onFailure: (String, ErrorData) => Unit, incMetric: (String, Long) => Unit): Unit = {

    if (event == null) {
      onFailure(event, ErrorData("EMPTY_JSON_EVENT", "Event data is null or empty"))
    } else if (!isValidJSON(event)) {
      onFailure(event, ErrorData("JSON_FORMAT_ERR", "Not a valid json"))
    } else {
      transformEvent(event, onSuccess)
    }
  }

  private def transformEvent(event: String, onSuccess: String => Unit) = {
    val sbKnowlgEvent = JSONUtil.deserialize[Map[String, AnyRef]](event)
    val data = sbKnowlgEvent.getOrElse("data", List.empty).asInstanceOf[List[Map[String, AnyRef]]]
    data.foreach {
      transaction => {
        val nodeUniqueId = transaction.getOrElse("nodeUniqueId", "").asInstanceOf[String]
        val transactionData = transaction.getOrElse("transactionData", Map.empty).asInstanceOf[Map[String, AnyRef]]
        val properties = transactionData.getOrElse("properties", Map.empty).asInstanceOf[Map[String, AnyRef]]
        val nodeData = mutable.Map[String, AnyRef]()
        properties.foreach {
          case (key, value) => nodeData.put(key, value.asInstanceOf[Map[String, AnyRef]].getOrElse("newValue", null))
        }
        nodeData.put("identifier", nodeUniqueId)
        onSuccess(JSONUtil.serialize(nodeData))
      }
    }
  }

  private def isValidJSON(json: String): Boolean = {
    try {
      new JSONObject(json)
      true
    }
    catch {
      case _: JSONException =>
        false
    }
  }


  override def getMetrics(): List[String] = List[String]()
}
