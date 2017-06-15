package com.canal

import java.net.InetSocketAddress
import com.alibaba.otter.canal.client.CanalConnector
import com.alibaba.otter.canal.client.CanalConnectors
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.{LoggerFactory, Logger}
/**
  * Created by xiaoft on 2016/12/27.
  */

object SimpleCanalClient{
  protected val logger: Logger = LoggerFactory.getLogger(classOf[SimpleCanalClient])
  def main(args: Array[String]) {
    if (args.length!=2){
      throw new RuntimeException("ip,destination must set")
    }

    val connector: CanalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(args(0), 11111), args(1), "", "")
    val clientTest: SimpleCanalClient = new SimpleCanalClient("")
    clientTest.setConnector(connector)
    clientTest.start
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run {
        try {
          SimpleCanalClient.logger.info("## stop the canal client")
          clientTest.stop
        }
        catch {
          case e: Throwable => {
            SimpleCanalClient.logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e))
          }
        } finally {
          SimpleCanalClient.logger.info("## canal client is down.")
        }
      }
    })
  }
}

class SimpleCanalClient (destination: String) extends AbstractCanalClient(destination: String) {


}

