
import java.text.SimpleDateFormat

import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object JsonUtil {

  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(new JodaModule())
  mapper.disable(FAIL_ON_UNKNOWN_PROPERTIES)
  mapper.setSerializationInclusion(NON_NULL)
  mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

  def toJson(value: Any): String = mapper.writeValueAsString(value)

  def toByteArrayJson(value: Any): Array[Byte] = mapper.writeValueAsBytes(value)

  def fromJson[T](json: Array[Byte])(implicit m: Manifest[T]): T = {
    mapper.readValue[T](json)
  }

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = mapper.readValue[T](json)

  def readNode[T](jsonNode: JsonNode)(implicit m: Manifest[T]): T = mapper.treeToValue(jsonNode)

  def getMapper: ObjectMapper with ScalaObjectMapper = mapper

  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    Map(
      "schema" -> "iglu:uk.co.simplybusiness/pooka_entity/jsonschema/1-0-0",
      "data" -> Map(

      )
    )

    fromJson[Map[String, Any]](jsonStr)
  }

  def jsonToEntity(jsonStr: String): Entity = {
    fromJson[Entity](jsonStr)
  }

  //  {
  //    "schema": "iglu:uk.co.simplybusiness/pooka_entity/jsonschema/1-0-0",
  //    "data": {
  //      "id": "5a7d85439ab3402003b88b9e",
  //      "entity": "chopin_sales_ledger_transactions",
  //      "version": 2,
  //      "source": "chopin",
  //      "payload": {
  //      "event": "Cancellation",
  //      "source": "Bank",
  //      "channel": "Offline",
  //      "reason": "",
  //      "comment": null,
  //      "author": "Laura Caswell",
  //      "amount": "-9.79",
  //      "created_at": "2018-02-09T11:25:55.763Z",
  //      "accounted_at": "2018-02-09T11:25:55.715Z",
  //      "transaction_type": "Payment",
  //      "policy_transaction_account_id": "5a7451679ab340715392d020",
  //      "policy_segment_id": "5a7451679ab340715392cf17_5a7451679ab340715392cf18",
  //      "payment_method": "BACS",
  //      "payment_reference": "",
  //      "creation_method": null
  //    },
  //      "published_at": "2018-02-09T11:25:55.767184Z"
  //    }
  case class Entity(schema: String, data: EntityData)

  case class EntityData(id: String, entity: String, version: Int, source: String, payload: Map[String, Any])

}

