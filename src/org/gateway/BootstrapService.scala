package org.gateway

import java.util.concurrent.CountDownLatch
import org.slf4j.LoggerFactory
import org.gateway.Extensions._
import scala.collection.mutable.ArrayBuffer

object BootstrapService {

  import org.gateway.Routes.fail

  private def logger = LoggerFactory.getLogger("BootstrapService")
  val toInt = (arg:String) => { try { Some(Integer.parseInt(arg.trim)) } catch { case e: Exception => None }}

  val router: PartialFunction[Route, Boolean] = {
    //case Route("GET", "order", clientId) => true
    case Route("POST", "order", clientId) => true
  }

  val latch = new CountDownLatch(1)

  def main(args: Array[String]) {

    //default args
    val defaultArgs = Array[String]("localhost", "9000")

    for (i <- 0 to args.length - 1) {
      if (i > 1) { logger.debug(s"Max params size is 2 "); }
      logger.debug(s"override default param: ${i} ${args(i)}")
      defaultArgs(i) = args(i)
    }

    val host = defaultArgs(0)
    val port = defaultArgs(1) toInt

    logger.debug(s" host: ${host}  port: ${port}")

    val server = new GatewayNettyHttpServer(host, port, router.orElse(fail))

    try {
      server.bind
      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        try {
          logger.info("JVM shutdownHook FrontendBootstrap.")
          latch.countDown
        } catch {
          case ex: Exception => {
            latch.countDown
            logger.debug("FrontendBootstrap was stopped with error: " + ex.getMessage)
          }
        }
      }))
      latch.await()
    } finally {
      server.shutdown
      logger.info("FrontendBootstrap was stopped")
    }
  }
}