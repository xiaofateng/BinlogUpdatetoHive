package com.hive

import org.apache.hive.hcatalog.streaming.DelimitedInputWriter
import org.apache.hive.hcatalog.streaming.HiveEndPoint
import org.apache.hive.hcatalog.streaming.StreamingConnection
import org.apache.hive.hcatalog.streaming.TransactionBatch
import java.util.ArrayList

/**
  * Created by xiaoft on 2017/2/7.
  */
object StreamingHive {
  @throws(classOf[Exception])
  def main(args: Array[String]) {
    val dbName: String = "tmp"
    val tblName: String = "alerts"

    val partitionVals: ArrayList[String] = new ArrayList[String](2)
    partitionVals.add("Asia")
    partitionVals.add("India")

    val serdeClass: String = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    val hiveEP: HiveEndPoint = new HiveEndPoint("thrift://ip:9083", dbName, tblName, partitionVals)
    val connection: StreamingConnection = hiveEP.newConnection(true)
    val writer: DelimitedInputWriter = new DelimitedInputWriter(Array[String]("id", "msg"), ",", hiveEP)

    var txnBatch: TransactionBatch = connection.fetchTransactionBatch(10, writer)
    txnBatch.beginNextTransaction
    txnBatch.write("1,Hello streaming".getBytes)
    txnBatch.write("2,Welcome to streaming".getBytes)
    txnBatch.commit
    if (txnBatch.remainingTransactions > 0) {
      txnBatch.beginNextTransaction
      txnBatch.write("3,Roshan Naik".getBytes)
      txnBatch.write("4,Alan Gates".getBytes)
      txnBatch.write("5,Owen O’Malley".getBytes)
      txnBatch.commit
      txnBatch.close
      connection.close
    }

    txnBatch = connection.fetchTransactionBatch(10, writer)
    txnBatch.beginNextTransaction
    txnBatch.write("6,David Schorow".getBytes)
    txnBatch.write("7,Sushant Sowmyan".getBytes)
    txnBatch.commit
    if (txnBatch.remainingTransactions > 0) {
      txnBatch.beginNextTransaction
      txnBatch.write("8,Ashutosh Chauhan".getBytes)
      txnBatch.write("9,Thejas Nair".getBytes)
      txnBatch.commit
      txnBatch.close
    }
    connection.close
  }
}

