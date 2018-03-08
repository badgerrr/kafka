import java.util

import JsonUtil.Entity
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class EntitySerde extends Serde[Entity] {

  override def deserializer(): Deserializer[Entity] = new Deserializer[Entity] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): Entity = JsonUtil.fromJson[Entity](data)
  }

  override def serializer(): Serializer[Entity] = new Serializer[Entity] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: Entity): Array[Byte] = JsonUtil.toByteArrayJson(data)

    override def close(): Unit = {}
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}

class MapSerde extends Serde[Map[String, Any]] {

  override def deserializer(): Deserializer[Map[String, Any]] = new Deserializer[Map[String, Any]] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): Map[String, Any] = JsonUtil.fromJson[Map[String, Any]](data)
  }

  override def serializer(): Serializer[Map[String, Any]] = new Serializer[Map[String, Any]] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: Map[String, Any]): Array[Byte] = JsonUtil.toByteArrayJson(data)

    override def close(): Unit = {}
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}