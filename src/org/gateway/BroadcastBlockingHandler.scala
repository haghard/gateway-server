package org.gateway

import io.netty.channel._
import org.slf4j.LoggerFactory
import io.netty.buffer.Unpooled
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioDatagramChannel
import java.net.InetSocketAddress
import io.netty.channel.ChannelHandler.Sharable

@Sharable
class BroadcastBlockingHandler extends
                          SimpleChannelInboundHandler[Order](classOf[Order]) {

  private val bootstrap = new Bootstrap()
  private val group = new NioEventLoopGroup(1, new NamedThreadFactory("broadcast-group"))
  private val outboundChannel = broadcastOutboundChannel

  private val logger = LoggerFactory.getLogger(classOf[BroadcastBlockingHandler])

  private def broadcastOutboundChannel() = {
    //not sure about synchronized, but since it's @Sharable
    this.synchronized {
      bootstrap.group(group)
        .channel(classOf[NioDatagramChannel])
        .option(ChannelOption.SO_BROADCAST, java.lang.Boolean.TRUE)
        .handler(new Encoder(new InetSocketAddress("255.255.255.255", 9090)))
      bootstrap.bind(0).sync.channel
    }
  }

  override def channelRead0(ctx: ChannelHandlerContext, order: Order) = {
    //logger.debug(s"before broadcast ${hashCode}")
    outboundChannel.writeAndFlush(order)/*.addListener(CLOSE)*/
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    logger.debug("BroadcastHandler.Exception: " + cause.getMessage)
    ctx.close()
  }

  def shutdownBroadcast {
    logger.debug("close broadcast")
    if (outboundChannel.isActive) {
      outboundChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    } else {
      outboundChannel.close()
    }
    group.shutdownGracefully().sync()
  }
}