package com.canal

import com.alibaba.otter.canal.client.CanalConnector
import com.alibaba.otter.canal.client.CanalConnectors
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.{LoggerFactory, Logger}
/**
  * Created by xiaoft on 2016/12/27.
  */

object ClusterCanalClient{
  protected val logger: Logger = LoggerFactory.getLogger(classOf[ClusterCanalClient])

  def main(args: Array[String]) {
    if (args.length!=2){
      throw new RuntimeException("zookeeper,destination must set")
    }

    val connector: CanalConnector = CanalConnectors.newClusterConnector(args(0), args(1), "", "")
    val clientTest: ClusterCanalClient = new ClusterCanalClient(args(1))
    clientTest.setConnector(connector)
    clientTest.start
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run {
        try {
          ClusterCanalClient.logger.info("## stop the canal client")
          clientTest.stop
        }
        catch {
          case e: Throwable => {
            ClusterCanalClient.logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e))
          }
        } finally {
          ClusterCanalClient.logger.info("## canal client is down.")
        }
      }
    })
  }

}

class ClusterCanalClient(destination: String) extends AbstractCanalClient(destination: String){

}
