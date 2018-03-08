package com.jack.kafka

object ProducerRunner extends App {

  val salesLedgerProducer = new CreateProducer("SalesLedger", "localhost:9092", "SalesLedgerTestData.json")

  val coverTransactionProducer = new CreateProducer("CoverTransaction", "localhost:9092", "CoverTransactionTestData.json")

  val policySegmentProducer = new CreateProducer("PolicySegment","localhost:9092", "PolicySegmentTestData.json")

  val policyChainProducer = new CreateProducer("PolicyChain","localhost:9092", "PolicyChainEntriesTestData.json")

}

