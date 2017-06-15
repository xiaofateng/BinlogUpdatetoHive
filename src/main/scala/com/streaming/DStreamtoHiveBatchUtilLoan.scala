package com.streaming

import java.util.Arrays

import com.test._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.ql.io.RecordIdentifier
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClientBuilder
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinatorBuilder
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.JavaConverters._



/**
  * Created by xiaoft on 16/6/17.
  * 此类,使用HCatalog Streaming Mutation API向hive2中进行实时增删改数据.
  * 注意,insert操作和update操作需要开启两个完全独立的Transaction和Coordinator
  *
  */
object DStreamtoHiveBatchUtilLoan {

  def toHive(dStream:DStream[String]):Unit = {
    dStream.foreachRDD{
      rdd=> rdd.foreachPartition {
        partitionOfRecords =>

          val metaStoreUri: String = "thrift://ip:2183"
          val databaseName: String = "tmp"
          val tableName: String = "bdl_rrxdebt_loan_order"
          val createPartitions: Boolean = false
          val EUROPE_FRANCE = Arrays.asList("2017", "02")
          val hbaseTableName: String = tableName+"_identifier"


          var conf: HiveConf = null
          val testUtils: StreamingTestUtils = new StreamingTestUtils
          var metaStoreClient: IMetaStoreClient = null
          val BUCKET_COLUMN_INDEXES: Array[Int] = Array[Int](0)
          val RECORD_ID_COLUMN: Int = 33
          var assertionFactory: StreamingAssert.Factory = null
          //在RDD分区中创建hive连接

          conf = testUtils.newHiveConf(metaStoreUri);
          testUtils.prepareTransactionDatabase(conf);
          metaStoreClient = testUtils.newMetaStoreClient(conf);

          //在RDD分区中创建hbase连接
          val hConfig:Configuration = HBaseConfiguration.create()
          hConfig.set("hbase.zookeeper.quorum", "ip1,ip2,ip3");
          hConfig.setInt("hbase.zookeeper.property.clientPort", 2181);

          val hbaseConnection :Connection= ConnectionFactory.createConnection(hConfig)
          val hbaseTable :Table = hbaseConnection.getTable(TableName.valueOf(hbaseTableName))
          var puts:ListBuffer[Put]= ListBuffer[Put]()

          assertionFactory = new StreamingAssert.Factory(metaStoreClient, conf);

          val insertClient = new MutatorClientBuilder()
            .addSinkTable(databaseName, tableName, createPartitions)
            .metaStoreUri(metaStoreUri)
            .build();
          insertClient.connect();

          val tables = insertClient.getTables();

          val insertTransaction = insertClient.newTransaction();

          insertTransaction.begin();
          val transActionid = insertTransaction.getTransactionId();
          System.out.println("insertTransActionid===" + transActionid);

          var row_id=0

          val mutatorFactory = new ReflectiveMutatorFactory(conf, classOf[LoanorderRecord], RECORD_ID_COLUMN,
            BUCKET_COLUMN_INDEXES);

          val bucketIdResolver = mutatorFactory.newBucketIdResolver(tables.get(0).getTotalBuckets());

          val insertCoordinator = new MutatorCoordinatorBuilder()
            .metaStoreUri(metaStoreUri)
            .table(tables.get(0))
            .mutatorFactory(mutatorFactory)
            .build();

          var updateArray=ArrayBuffer[LoanorderRecord]()

          try {
            partitionOfRecords.foreach(
              rdd => {
                {
                  //println(rdd)
                  if(rdd.toString.split(",").length>33){
                    System.out.println("rdd_data "+rdd.toString.split(",")(0)+"==="+rdd.toString.split(",")(1))
                    //解析记录
                    var rddAry=rdd.toString.split(",")
                    val eventType=rddAry(0)
                    val asiaIndiaRecord1 =  bucketIdResolver.attachBucketIdToRecord(
                      new LoanorderRecord(
                        rddAry(1), rddAry(2), rddAry(3),
                        rddAry(4), rddAry(5), rddAry(6),
                        rddAry(7), rddAry(8), rddAry(9),
                        rddAry(10), rddAry(11), rddAry(12),
                        rddAry(13), rddAry(14), rddAry(15),
                        rddAry(16), rddAry(17), rddAry(18),
                        rddAry(19), rddAry(20), rddAry(21),
                        rddAry(22), rddAry(23), rddAry(24),
                        rddAry(25), rddAry(26), rddAry(27),
                        rddAry(28), rddAry(29), rddAry(30),
                        rddAry(31), rddAry(32),rddAry(33)
                      )).asInstanceOf[LoanorderRecord]

                    if (eventType.toString().equals("INSERT")) {
                      row_id+=1
                      insertCoordinator.insert(EUROPE_FRANCE, asiaIndiaRecord1)
                      //put ROW_ID to hbase
                      val put: Put = new Put(Bytes.toBytes(rddAry(1)))
                      put.addColumn(Bytes.toBytes("fl"), Bytes.toBytes("identifier"), Bytes.toBytes(transActionid + "_" + "0_" + (row_id)))
                      puts += put
                    }
                    //需要立即执行put操作,将数据put到表中,因为这一批中下一条记录,可能就是对刚插进去数据的修改.
                    hbaseTable.put(puts.toList.asJava)

                    if (eventType.toString().equals("UPDATE")) {
                      println("\n -------update-------- \n")
                      //从hbase中获取RecordIdentifier
                      val get:Get=new Get(Bytes.toBytes(rddAry(1).toString))
                      val result = hbaseTable.get(get)
                      val rowId = Bytes.toString(result.getValue(Bytes.toBytes("fl"), Bytes.toBytes("identifier")))
                      val recordIdentifierAry=rowId.split("_")

                      println("Id"+rddAry(1).toString+"---rowId---"+rowId)

                      updateArray+=new LoanorderRecord(
                        rddAry(1), rddAry(2), rddAry(3),
                        rddAry(4), rddAry(5), rddAry(6),
                        rddAry(7), rddAry(8), rddAry(9),
                        rddAry(10), rddAry(11), rddAry(12),
                        rddAry(13), rddAry(14), rddAry(15),
                        rddAry(16), rddAry(17), rddAry(18),
                        rddAry(19), rddAry(20), rddAry(21),
                        rddAry(22), rddAry(23), rddAry(24),
                        rddAry(25), rddAry(26), rddAry(27),
                        rddAry(28), rddAry(29), rddAry(30),
                        rddAry(31), rddAry(32),rddAry(33),new RecordIdentifier(recordIdentifierAry(0).toLong, recordIdentifierAry(1).toInt, recordIdentifierAry(2).toLong)
                      )
                    }
                  }
                  else {
                    System.out.println("\n\n\nrdd_data < 33\n\n\n")
                  }
                }
              })
          } catch {
            case e: Exception => e.printStackTrace
          }
          finally {
            //System.out.println("asiaIndiaRecord1.rowId=======" + asiaIndiaRecord1.rowId.toString());
            //insert commit
            insertCoordinator.close()
            insertTransaction.commit()
            insertClient.close();
            if(hbaseTable!=null){
              hbaseTable.close()}
            if(hbaseConnection!=null){
              hbaseConnection.close()}
          }

        //update操作在insert操作完成之后进行
        val updateClient = new MutatorClientBuilder()
          .addSinkTable(databaseName, tableName, createPartitions)
          .metaStoreUri(metaStoreUri)
          .build();
          updateClient.connect();

        val tables2 = updateClient.getTables();
        val updateTransaction = updateClient.newTransaction();
        updateTransaction.begin();

        val updateCoordinator = new MutatorCoordinatorBuilder()
        .metaStoreUri(metaStoreUri)
        .table(tables2.get(0))
        .mutatorFactory(mutatorFactory)
        .build();
        // Sorting
        val sortedUpdateArray=updateArray.sortBy(r => (r.rowId.getTransactionId, r.rowId.getRowId))

        for(r <- sortedUpdateArray){
          println("\n----update--r-------\n"+r.toString)
          updateCoordinator.update(EUROPE_FRANCE,r)
        }

        updateCoordinator.close();
        updateTransaction.commit();
        updateClient.close()


}
}
}
}
