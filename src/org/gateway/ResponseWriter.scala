package org.gateway

import io.netty.channel._
import io.netty.handler.codec.http._
import org.slf4j.LoggerFactory

class ResponseWriter extends ChannelInboundHandlerAdapter {

  private val logger = LoggerFactory.getLogger(classOf[ResponseWriter])

  override def channelRead(ctx: ChannelHandlerContext, msg: Object) {
    msg match {
      case response: FullHttpResponse => {
        if (HttpHeaders.isKeepAlive(response)) {
          response.headers().set(HttpHeaders.Names.CONTENT_LENGTH,
            response.content().readableBytes())
          response.headers().set(HttpHeaders.Names.CONNECTION,
            HttpHeaders.Values.KEEP_ALIVE)
          ctx.writeAndFlush(msg)
        } else {
          ctx.writeAndFlush(msg).addListener(ChannelFutureListener.CLOSE)
        }
      }

      case _ => logger.debug("unknown response type in writer");
    }
  }

  override def exceptionCaught(ctx :ChannelHandlerContext, cause: Throwable) {
    logger.debug("RequestWriter.Exception: " + cause.getMessage)
    ctx.close();
  }
}
