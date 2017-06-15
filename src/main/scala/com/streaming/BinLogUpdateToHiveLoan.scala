package com.streaming

import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
/**
  * Created by xiaoft on 16/8/5.
  */

object BinLogUpdateToHiveLoan {
  var path="12"
  def functionToCreateContext(checkpointDirectory: String): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("test_BinLogUpdateToHive")

    var interval = 15
    val ssc = new StreamingContext(sparkConf, Seconds(interval))
    ssc.checkpoint(checkpointDirectory)

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "ip:9092")


    //val topicsSet = Set("canaltopic")
    val topicsSet = Set("test")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //messages.print()

    val lines = messages.map(_._2)

    //println("lines---"+lines.toString)

    //lines.print()

    val sqls=lines.map(line=>
    {
      var database=""
      var table=""
      var evenType=""

      val length=line.split("\t").length
      val builder = new scala.collection.mutable.StringBuilder

      /*
      line 数据样例
      msg = database=test1,table=pay_flow,eventType=INSERT
      id=1234566,type=bigint(20),update=true
      uuid=1111111111112222222,type=varchar(36),update=true
       */
      if(length>=2){
        database=line.split("\t")(0).split(",")(0).split("=")(1)
        table=line.split("\t")(0).split(",")(1).split("=")(1)
        evenType=line.split("\t")(0).split(",")(2).split("=")(1)
      }

      //evenType.equals("INSERT")&&
      if (table.contains("loan_order")){
        //values
        builder.append(evenType+",")
        for(ele <-line.split("\t")){
          if(!ele.contains("eventType")&&ele.toString.split(",").length>1){
            if(ele.toString().split(",").length>1){
              if(ele.toString().split(",")(0).split("=").length==2){
                builder.append(ele.toString().split(",")(0).split("=")(1))
                builder.append(",")
              }
              else {
                builder.append("")
                builder.append(",")
              }
            }
          }
        }
        builder.deleteCharAt(builder.toString().lastIndexOf(","))
      }
      builder.toString()

    })

    val sqls2=sqls.filter(sql=>sql.length>1)

    //sqls2.print()

    DStreamtoHiveBatchUtilLoan.toHive(sqls2)

    ssc
  }


  def main(args: Array[String]) = {
    if(args.length>0)
      path = args(0)
    var checkpointDirectory="/tmp/"+path+"/test_BinLogUpdate/checkpoint"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => {functionToCreateContext(checkpointDirectory)})
    ssc.start()
    ssc.awaitTermination()

  }
}