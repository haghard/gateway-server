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
import io.netty.util.concurrent.{ImmediateEventExecutor, DefaultEventExecutorGroup}

final class GatewayNettyHttpServer(val host: String, val port: Int) {

  private var channel: Channel = _
  private val broadcast = new BroadcastBlockingHandler()

  private val bossEventLoopGroup: EventLoopGroup =
    new NioEventLoopGroup(1, new NamedThreadFactory("boss"))

  private val workerEventLoopGroup: EventLoopGroup =
    new NioEventLoopGroup(8, new NamedThreadFactory("worker"))

  private lazy val bootstrap: ServerBootstrap = {

    /*http://brikis98.blogspot.ru/2014/02/maxing-out-at-50-concurrent-connections.html*/
    //sudo sysctl -w kern.ipc.somaxconn=1024
    val backlog =
      Option(System.getProperty("http.netty.backlog"))
        .map(Integer.parseInt)
        .getOrElse(4096)

    logger.debug(s"Backlog: ${backlog}")

    new ServerBootstrap()
      .group(bossEventLoopGroup, workerEventLoopGroup)
      .option[Integer](ChannelOption.valueOf("backlog"), backlog)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ServerInitializer())
      .localAddress(new InetSocketAddress(host, port))
  }

  private def logger = LoggerFactory.getLogger(classOf[GatewayNettyHttpServer])
  private val clients: ChannelGroup = new DefaultChannelGroup("http-clients", ImmediateEventExecutor.INSTANCE)

  private val blockingExecutorGroup =
    new DefaultEventExecutorGroup(1, new NamedThreadFactory("broadcaster"));

  /**
   *
   */
  def bind {
    val future = bootstrap.bind.sync()
    channel = future.channel
    if (future.isSuccess) {
      logger.debug("GatewayNettyHttpServer was started")
    } else {
      logger.debug("GatewayNettyHttpServer started error")
    }
  }

  /**
   *
   */
  def shutdown {
    logger.debug(s"Current client size: ${clients.size}")

    broadcast.shutdownBroadcast
    val it = clients.iterator
    while (it.hasNext) {
      val channel = it.next
      channel.close().awaitUninterruptibly()
      logger.debug(s"Client channel ${channel} was closed")
    }

    if (channel != null) {
      channel.close.awaitUninterruptibly
    }

    bossEventLoopGroup.shutdownGracefully().sync
    workerEventLoopGroup.shutdownGracefully().sync
    blockingExecutorGroup.shutdownGracefully().sync
    logger.debug("GatewayNettyHttpServer was stopped")
  }

  final class ServerInitializer() extends ChannelInitializer[SocketChannel] {

    override def initChannel(ch: SocketChannel) {
      logger.debug(s"init channel ${ch.remoteAddress}")

      val pipe = ch.pipeline

      clients.add(ch)

      //pipe.addLast("logger", new LoggingHandler(LogLevel.DEBUG));

      pipe.addLast("http", new HttpServerCodec)

      pipe.addLast("deflater", new HttpContentCompressor)

      pipe.addLast("aggegator", new HttpObjectAggregator(512 * 1024));

      //pipe.addLast("decoder", new HttpRequestDecoder());
      //pipe.addLast("encoder", new HttpResponseEncoder());

      pipe.addLast("router", new EchoHttpHandler())

      pipe.addLast(blockingExecutorGroup, broadcast)

      //pipe addLast("response-writer", new HttpResponseWriter)

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