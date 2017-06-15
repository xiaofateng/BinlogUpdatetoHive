package com.canal

/**
  * Created by xiaoft on 2016/10/25.
  */

import java.util.{Date, Properties}
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import java.util.Date
import java.util.Properties

/**
  * 一个简单的Kafka Producer类，传入两个参数：
  * topic num
  * 设置主题和message条数
  *
  * 执行过程：
  * 1、创建一个topic
  * kafka-topic.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic xxxx
  * 2、运行本类中的代码
  * 3、查看message
  * kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic xxxx
  * kafka
  */
object SimpleKafkaProducer {
  /**
    * Producer的两个泛型，第一个指定Key的类型，第二个指定value的类型
    */
  private var producer: Producer[String, String] = null
}

class SimpleKafkaProducer {

    val props: Properties = new Properties
    props.put("metadata.broker.list", "localhost:9092")

  props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    val config: ProducerConfig = new ProducerConfig(props)
    SimpleKafkaProducer.producer = new Producer[String, String](config)

  /**
    * 根据topic和消息条数发送消息
    *
    * @param topic
    * @param count
    */
  def publishMessage(topic: String, count: Int) {
    {
      var i: Int = 0
      while (i < count) {
        {
          val runtime: String = new Date().toString
          val msg: String = "Message published time - " + runtime
          System.out.println("msg = " + msg)
          val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, msg)
          SimpleKafkaProducer.producer.send(data)
        }
        ({
          i += 1; i - 1
        })
      }
    }
    SimpleKafkaProducer.producer.close
  }

  def publishMessage(topic: String, message: String) {
    val runtime: String = new Date().toString
    val msg: String = message
    //System.out.println("runtime= " + runtime + "   msg = " + msg)
    val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, msg)
    SimpleKafkaProducer.producer.send(data)
    SimpleKafkaProducer.producer.close
  }
}

