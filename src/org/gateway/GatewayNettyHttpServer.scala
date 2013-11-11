package org.gateway

import io.netty.channel.nio.NioEventLoopGroup

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http._
import java.net.InetSocketAddress
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import org.slf4j.LoggerFactory
import io.netty.channel.group.{DefaultChannelGroup, ChannelGroup}
import io.netty.util.concurrent.{DefaultEventExecutorGroup, GlobalEventExecutor}
import io.netty.handler.logging.{LogLevel, LoggingHandler}

class GatewayNettyHttpServer(val host: String,
                         val port: Int,
                         router: PartialFunction[Route, Boolean]) {

  private var channel: Channel = _
  private val rHandler = new RoutingHandler(router)
  private val bossEventLoopGroup: EventLoopGroup = new NioEventLoopGroup(2, new NamedThreadFactory("boss"))
  private val workerEventLoopGroup: EventLoopGroup = new NioEventLoopGroup(4, new NamedThreadFactory("worker"))
  private lazy val bootstrap: ServerBootstrap = {
    new ServerBootstrap().group(bossEventLoopGroup, workerEventLoopGroup).
      channel(classOf[NioServerSocketChannel])
      .childHandler(new ServerInitializer(router))
      .localAddress(new InetSocketAddress(host, port))
  }

  private def logger = LoggerFactory.getLogger(classOf[GatewayNettyHttpServer])
  private val clients: ChannelGroup = new DefaultChannelGroup("http-clients", GlobalEventExecutor.INSTANCE)
  private val blockingExecutor = new DefaultEventExecutorGroup(1, new NamedThreadFactory("broadcaster worker"));

  /**
   *
   */
  def bind {
    val future = bootstrap.bind.sync()
    channel = future.channel()
    if (future.isSuccess) {
      logger.debug("FrontEndServer was started")
    } else {
      logger.debug("FrontEndServer started error")
    }
  }

  /**
   *
   */
  def shutdown {
    //
    rHandler.shutdownBroadcast

    val it = clients.iterator()
    while (it.hasNext) {
      val channel = it.next()
      channel.close().awaitUninterruptibly()
      logger.debug(s"Client channel ${channel} was closed")
    }

    if (channel != null) channel.close.awaitUninterruptibly()

    bossEventLoopGroup.shutdownGracefully.sync()
    workerEventLoopGroup.shutdownGracefully().sync()
    blockingExecutor.shutdownGracefully().sync()
    logger.debug("FrontEndServer was stopped")
  }

  class ServerInitializer(router: PartialFunction[Route, Boolean]) extends ChannelInitializer[SocketChannel] {
    override def initChannel(ch: SocketChannel) {
      val pipe = ch.pipeline

      clients.add(ch)

      //pipe.addLast("logger", new LoggingHandler(LogLevel.DEBUG));

      pipe.addLast("http", new HttpServerCodec)

      //pipe.addLast("deflater", new HttpContentCompressor)

      pipe.addLast("aggegator", new HttpObjectAggregator(512 * 1024));

      //pipe.addLast("decoder", new HttpRequestDecoder());
      //pipe.addLast("encoder", new HttpResponseEncoder());

      pipe.addLast(blockingExecutor, rHandler)
      //pipe.addLast("router", new RoutingHandler(router))

      pipe.addLast("response-writer", new ResponseWriter)

      /**
       * There's no replacement for MemoryAwareThreadPoolExecutor because all handler methods in Netty 4 are
       * invoked sequentially for the same connection. If you want unordered execution, you'll have to hand off
       * the tasks to a java.util.concurrent.Executor.
       * This decision is intentional - otherwise a handler implementation cannot make any consumption about thread safety.
       */
      //EventExecutorGroup executor = new DefaultEventExecutorGroup(new NamedThreadFactory("db writer worker"));
      //pipe.addLast(executor, new MyHandler());
    }
  }
}