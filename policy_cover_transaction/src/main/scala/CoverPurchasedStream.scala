
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import java.util.Properties
import java.util.concurrent.TimeUnit

import JsonUtil._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams.KStreamS


object KeyValueImplicits {

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

}

object CoverPurchasedStream {

  type SMap = KStreamS[String, Map[String, Any]]

  val stringSerde: Serde[String] = Serdes.String()
  val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]
  val entitySerde = new EntitySerde
  val mapSerde = new MapSerde
  val builder: StreamsBuilder = new StreamsBuilder()
  val joinWindowTime = JoinWindows.of(TimeUnit.MINUTES.toMillis(5))

  def main(args: Array[String]) {

    val firstJoin = joinCoverTransactionsWithSalesLedger
    val secondJoin = joinCoverSalesWithSegment(firstJoin)
    val thirdJoin = joinCoverSegmentWithChain(secondJoin)

    val policyCoverTransaction = thirdJoin.map((k, v) => (k, toJson(v)))

    policyCoverTransaction.to("PolicyCoverTransaction", Produced.`with`(stringSerde, stringSerde)) //Produced.with(stringSerde, longSerde)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), configure)
    streams.start()
  }

  private def configure: Properties = {

    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "cover-purchased-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, mapSerde.getClass)
    p

  }

  private def joinCoverTransactionsWithSalesLedger: SMap = {

    val coverTransactionStream: KStreamS[String, Entity] = builder.stream("CoverTransaction", Consumed.`with`(stringSerde, entitySerde))
    val salesLedgerStream: KStreamS[String, Entity] = builder.stream("SalesLedger", Consumed.`with`(stringSerde, entitySerde))

    val coverTransactionStreamKV: KStreamS[String, Entity] = coverTransactionStream
      .map((_, value) => (value
        .data
        .payload("sales_ledger_transaction_id")
        .asInstanceOf[String]
        , value))

    val salesLedgerStreamKV: KStreamS[String, Entity] = salesLedgerStream
      .map((_, value) => (value.data.id, value))

    coverTransactionStreamKV
      .join(salesLedgerStreamKV, (left: Entity, right: Entity) => {

        val leftJsonMap = left.data.payload

        val rightJsonMap = right
          .data
          .payload
          .filterKeys(key => Set("channel", "author", "created_at", "accounted_at", "policy_segment_id", "capacity_provider", "insurer")
            .contains(key))

        leftJsonMap ++ rightJsonMap
      },
        joinWindowTime,
        Joined.`with`(stringSerde, entitySerde, entitySerde)

      )
  }

  private def joinCoverSalesWithSegment(joinCoverTransactionsWithSalesLedger: SMap): SMap = {

    val policySegmentStream: KStreamS[String, Entity] = builder.stream("PolicySegment", Consumed.`with`(stringSerde, entitySerde))

    val policySegmentStreamKV: KStreamS[String, Entity] = policySegmentStream.map((_, value) => (value.data.id, value))

    val coverSalesJoinedStreamKV: SMap = joinCoverTransactionsWithSalesLedger
      .map((_, value) => (value("policy_segment_id").asInstanceOf[String], value))

    coverSalesJoinedStreamKV
      .join(policySegmentStreamKV, (left: Map[String, Any], right: Entity) => {

        val rightJsonMap = right
          .data
          .payload
          .filterKeys(key => Set("rfq_id", "payment_ref", "payment_type", "payment_email_sent_at", "effective_start_date", "effective_end_date", "quote_id", "policy_id")
            .contains(key))

        left ++ rightJsonMap
      },
        joinWindowTime,
        Joined.`with`(stringSerde, mapSerde, entitySerde)
      )
  }

  private def joinCoverSegmentWithChain(joinCoverSalesWithSegment: SMap): SMap = {

    val policyChainStream: KStreamS[String, Entity] = builder.stream("PolicyChain", Consumed.`with`(stringSerde, entitySerde))

    val policyChainStreamKV = policyChainStream.map((_, value) => (value.data.payload("policy_id").asInstanceOf[String], value))

    val coverSegmentJoinedStreamKV: SMap = joinCoverSalesWithSegment
      .map((_, value) => (value("policy_id").asInstanceOf[String], value))

    coverSegmentJoinedStreamKV
      .join(policyChainStreamKV, (left: Map[String, Any], right: Entity) => {

        val rightJsonMap = right.data.payload
          .filterKeys(key => Set("policy_chain_id", "position_in_chain")
            .contains(key))

        left ++ rightJsonMap
      },
        joinWindowTime,
        Joined.`with`(stringSerde, mapSerde, entitySerde)
      )
  }
}
