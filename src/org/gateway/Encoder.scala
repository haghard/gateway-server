package org.gateway

import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.channel.ChannelHandlerContext
import java.net.InetSocketAddress
import org.slf4j.LoggerFactory
import io.netty.channel.socket.DatagramPacket
import io.netty.buffer.Unpooled

class Encoder(remoteAddress: InetSocketAddress) extends MessageToMessageEncoder[Order] {
  private def logger = LoggerFactory.getLogger(classOf[Encoder])

  override def encode(ctx: ChannelHandlerContext, req: Order, res: java.util.List[java.lang.Object]) {
    res.add(new DatagramPacket(Unpooled.copiedBuffer(Order.encode(req)), remoteAddress))
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    logger.debug("OrderEncoder: " + cause.getMessage)
    ctx.close()
  }
}
