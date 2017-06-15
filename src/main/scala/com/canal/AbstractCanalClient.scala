package com.canal

import java.text.SimpleDateFormat
import java.util.{Date, List}
import com.alibaba.otter.canal.client.CanalConnector
import com.alibaba.otter.canal.protocol.CanalEntry._
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.google.protobuf.InvalidProtocolBufferException
import org.apache.commons.lang.SystemUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.util.Assert
import org.springframework.util.CollectionUtils
import scala.collection.mutable.StringBuilder
import java.text.SimpleDateFormat
import java.util.Date
import java.util.List
import collection.JavaConversions._

/**
  * Created by xiaoft on 2016/10/25.
  * 这个类主要是:处理binlog日志的逻辑
  */

object AbstractCanalClient {
  protected val logger: Logger = LoggerFactory.getLogger(classOf[AbstractCanalClient])
  protected val SEP: String = SystemUtils.LINE_SEPARATOR
  protected val DATE_FORMAT: String = "yyyy-MM-dd HH:mm:ss"
  protected var context_format: String = null
  protected var row_format: String = null
  protected var transaction_format: String = null

  try {
    context_format = SEP + "****************************************************" + SEP
    context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP
    context_format += "* Start : [{}] " + SEP
    context_format += "* End : [{}] " + SEP
    context_format += "****************************************************" + SEP
    row_format = SEP + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms" + SEP
    transaction_format = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP
  }

}

class AbstractCanalClient {
  @volatile
  protected var running: Boolean = false
  protected var handler: Thread.UncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
    def uncaughtException(t: Thread, e: Throwable) {
      AbstractCanalClient.logger.error("parse events has an error", e)
    }
  }
  protected var thread: Thread = null
  protected var connector: CanalConnector = null
  protected var destination: String = null
  //记录处理数据的条数
  var records=0


  def this(destination: String) {
    this()
    this.destination = destination
    this.connector = null
  }

  def this(destination: String, connector: CanalConnector) {
    this()
    this.destination = destination
    this.connector = connector
  }

  protected def start {
    Assert.notNull(connector, "connector is null")
    thread = new Thread(new Runnable() {
      def run {
        process
      }
    })
    thread.setUncaughtExceptionHandler(handler)
    thread.start
    running = true
  }

  protected def stop {
    if (!running) {
      return
    }
    running = false
    if (thread != null) {
      try {
        thread.join
      }
      catch {
        case e: InterruptedException => {
        }
      }
    }
    MDC.remove("destination")
  }

  protected def process {
    val batchSize: Int = 5 * 1024

    while (running) {
      try {
        MDC.put("destination", destination)
        connector.connect
        connector.subscribe
        while (running) {
          val message: Message = connector.getWithoutAck(batchSize)
          val batchId: Long = message.getId
          val size: Int = message.getEntries.size
          if (batchId == -1 || size == 0) {
          }
          else {
            printSummary(message, batchId, size)
            printEntry(message.getEntries)
          }
          connector.ack(batchId)
        }
      }
      catch {
        case e: Exception => {
          AbstractCanalClient.logger.error("process error!", e)
        }
      } finally {
        connector.disconnect
        MDC.remove("destination")
      }
    }
  }

  private def printSummary(message: Message, batchId: Long, size: Int) {
    var memsize: Long = 0
    for (entry <- message.getEntries) {
      memsize += entry.getHeader.getEventLength
    }
    var startPosition: String = null
    var endPosition: String = null
    if (!CollectionUtils.isEmpty(message.getEntries)) {
      startPosition = buildPositionForDump(message.getEntries.get(0))
      endPosition = buildPositionForDump(message.getEntries.get(message.getEntries.size - 1))
    }
    val format: SimpleDateFormat = new SimpleDateFormat(AbstractCanalClient.DATE_FORMAT)
    //AbstractCanalClient.logger.info(AbstractCanalClient.context_format,  Array(batchId, size, memsize, format.format(new Date), startPosition, endPosition))
  }

  protected def buildPositionForDump(entry: CanalEntry.Entry): String = {
    val time: Long = entry.getHeader.getExecuteTime
    val date: Date = new Date(time)
    val format: SimpleDateFormat = new SimpleDateFormat(AbstractCanalClient.DATE_FORMAT)
    return entry.getHeader.getLogfileName + ":" + entry.getHeader.getLogfileOffset + ":" + entry.getHeader.getExecuteTime + "(" + format.format(date) + ")"
  }

  protected def printEntry(entrys: List[CanalEntry.Entry]) {
    import scala.collection.JavaConversions._


    for (entry <- entrys) {
      var isContinue=true
      var isContinue2=true

      val executeTime: Long = entry.getHeader.getExecuteTime
      val delayTime: Long = new Date().getTime - executeTime
      if (
        (entry.getEntryType eq EntryType.TRANSACTIONBEGIN )
          || (entry.getEntryType eq EntryType.TRANSACTIONEND)
      ) {
        if (entry.getEntryType eq EntryType.TRANSACTIONBEGIN) {
          var begin: CanalEntry.TransactionBegin = null
          try {
            begin = TransactionBegin.parseFrom(entry.getStoreValue)
          }
          catch {
            case e: InvalidProtocolBufferException => {
              throw new RuntimeException("parse event has an error , data:" + entry.toString, e)
            }
          }
          //AbstractCanalClient.logger.info(AbstractCanalClient.transaction_format, Array[AnyRef](entry.getHeader.getLogfileName, String.valueOf(entry.getHeader.getLogfileOffset), String.valueOf(entry.getHeader.getExecuteTime), String.valueOf(delayTime)))
          //AbstractCanalClient.logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId)
        }
        else if (entry.getEntryType eq EntryType.TRANSACTIONEND) {
          var end: CanalEntry.TransactionEnd = null
          try {
            end = TransactionEnd.parseFrom(entry.getStoreValue)
          }
          catch {
            case e: InvalidProtocolBufferException => {
              throw new RuntimeException("parse event has an error , data:" + entry.toString, e)
            }
          }
          //AbstractCanalClient.logger.info("----------------\n")
          //AbstractCanalClient.logger.info(" END ----> transaction id: {}", end.getTransactionId)
          //AbstractCanalClient.logger.info(AbstractCanalClient.transaction_format, Array[AnyRef](entry.getHeader.getLogfileName, String.valueOf(entry.getHeader.getLogfileOffset), String.valueOf(entry.getHeader.getExecuteTime), String.valueOf(delayTime)))
        }
        isContinue=false
      }

      if ((entry.getEntryType eq EntryType.ROWDATA)
        &&isContinue) {
        var rowChage: CanalEntry.RowChange = null
        try {
          rowChage = RowChange.parseFrom(entry.getStoreValue)
        }
        catch {
          case e: Exception => {
            throw new RuntimeException("parse event has an error , data:" + entry.toString, e)
          }
        }
        val eventType: CanalEntry.EventType = rowChage.getEventType
        //AbstractCanalClient.logger.info(AbstractCanalClient.row_format, Array[AnyRef](entry.getHeader.getLogfileName, String.valueOf(entry.getHeader.getLogfileOffset), entry.getHeader.getSchemaName, entry.getHeader.getTableName, eventType, String.valueOf(entry.getHeader.getExecuteTime), String.valueOf(delayTime)))
        if (
          (eventType eq EventType.QUERY )
            || rowChage.getIsDdl) {
          //val simpleProducer = new SimpleKafkaProducer()
          //simpleProducer.publishMessage("canaltopic", rowChage.getSql)
          //AbstractCanalClient.logger.info(" sql is xiaoft ----> " + rowChage.getSql + AbstractCanalClient.SEP)
          isContinue2=false
        }
        if (isContinue2){
          for (rowData <- rowChage.getRowDatasList) {
            if (eventType eq EventType.DELETE) {
              //AbstractCanalClient.logger.info("xiaosql delete")
              //val simpleProducer = new SimpleKafkaProducer()
              //simpleProducer.publishMessage("canaltopic", rowData.toString)
              printColumn(rowData.getBeforeColumnsList,entry,rowChage)
            }
            else if (eventType eq EventType.INSERT) {
              //AbstractCanalClient.logger.info("xiaosql insert")
              //AbstractCanalClient.logger.info("###rowData" + rowData)
              //AbstractCanalClient.logger.info("###rowChage.getsql" + rowChage.getSql)
              //AbstractCanalClient.logger.info("###rowChage.tostring" + rowChage.toString)
              //AbstractCanalClient.logger.info("### entry.getHeader().getSchemaName():" + entry.getHeader.getSchemaName)
              //AbstractCanalClient.logger.info("### entry.getHeader().getTableName():" + entry.getHeader.getTableName)
              //val simpleProducer =  new SimpleKafkaProducer()
              //simpleProducer.publishMessage("canaltopic", rowData.toString)
              printColumn(rowData.getAfterColumnsList,entry,rowChage)
            }
            else {
              //AbstractCanalClient.logger.info("xiaosql else")
              printColumn(rowData.getAfterColumnsList,entry,rowChage)
            }
          }
        }


      }
    }
  }

  protected def printColumn(columns: List[CanalEntry.Column],entry:CanalEntry.Entry,rowChage: CanalEntry.RowChange) {
    import scala.collection.JavaConversions._
    //builder 将binlog日志包装成希望的数据格式
    val builder: scala.collection.mutable.StringBuilder = new scala.collection.mutable.StringBuilder

    builder.append("database="+ entry.getHeader.getSchemaName)
    builder.append(",table="+ entry.getHeader.getTableName)
    var eventType=rowChage.getEventType

    builder.append(",eventType=" +eventType)

    for (column <- columns) {
      builder.append("\t"+column.getName + "=" + column.getValue)
      builder.append(",type=" + column.getMysqlType)
      if (column.getUpdated) {
        builder.append(",update=" + column.getUpdated)
      }else{
        builder.append(",update=false")
      }
    }

    val simpleProducer = new SimpleKafkaProducer()
    //simpleProducer.publishMessage("canaltopic",builder.toString())

    //只处理table 名字包含loan_order 的binlog信息

    if(entry.getHeader.getTableName.contains("loan_order")){

      if(eventType.toString().equals("INSERT")){
        records+=1
        AbstractCanalClient.logger.info("---在处理第n条insert数据---"+records)
      }

      //向kafka的test这个topic发送消息
      simpleProducer.publishMessage("test",builder.toString())

      AbstractCanalClient.logger.info(builder.toString+"\n\n\n")
    }

  }

  def setConnector(connector: CanalConnector) {
    this.connector = connector
  }
}
