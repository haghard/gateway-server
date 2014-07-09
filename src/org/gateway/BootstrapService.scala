package org.gateway

import java.util.concurrent.CountDownLatch
import org.slf4j.LoggerFactory
import org.gateway.Extensions._
import scala.util.Try

object BootstrapService {
  private def logger = LoggerFactory.getLogger("BootstrapService")
  val latch = new CountDownLatch(1)

  def main(args: Array[String]) {
    val host = Option(args(0)).getOrElse("localhost")
    val port = Option(Integer.parseInt(args(1).trim)).getOrElse(9000)

    logger.debug(s" host: ${host}  port: ${port}")
    val server = new GatewayNettyHttpServer(host, port)

    Try {
      server.bind
      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        Try({
          logger.info("JVM shutdownHook in BootstrapService")
          latch.countDown
        }).recover({
          case ex: Exception => {
            latch.countDown
            logger.debug("FrontendBootstrap was stopped with error: " + ex.getMessage)
          }
        }).get
      }))
      latch.await
    } recover({
      case e: Exception => {
        server.shutdown
        logger.info("BootstrapService was stopped")
      }
    })
  }
}