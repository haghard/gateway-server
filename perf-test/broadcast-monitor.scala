import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.channel._
import io.netty.channel.socket.DatagramPacket
import io.netty.handler.codec.MessageToMessageDecoder
import java.lang.Throwable
import java.net.InetSocketAddress
import java.util.Date
import org.gateway.Order

val group = new NioEventLoopGroup();
try {
val bootstrap = new Bootstrap
  bootstrap.group(group)
    .channel(classOf[NioDatagramChannel])
    .option(ChannelOption.SO_BROADCAST, java.lang.Boolean.TRUE)
    .handler(new ChannelInitializer[Channel]() {
        override def initChannel(channel: Channel) {
          val pipeline: ChannelPipeline  = channel.pipeline()
          pipeline.addLast(new LogEventDecoder())
          pipeline.addLast(new LogEventHandler())
        }
  }).localAddress(new InetSocketAddress(9090))


val channel = bootstrap.bind().syncUninterruptibly().channel()
println("Server started")
channel.closeFuture().await()
} finally {
  group.shutdownGracefully().sync()
  println("Server stopped")
}


class LogEventDecoder extends MessageToMessageDecoder[DatagramPacket] {
  override def decode(ctx: ChannelHandlerContext, packet: DatagramPacket, out: java.util.List[Object]){
    val byteBuf = packet.content()
    val bytes = new Array[Byte](byteBuf.readableBytes())
    byteBuf.getBytes(0, bytes)
    val order = Order.decode(bytes)
    out.add((order, System.currentTimeMillis()))
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace();
    ctx.close();
  }
}


class LogEventHandler extends SimpleChannelInboundHandler[(Order, Long)] {
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace();
    ctx.close();
  }

  override def channelRead0(ctx: ChannelHandlerContext, order: (Order, Long)) {
    val builder = new StringBuilder();
    builder.append(" [");
    builder.append(new Date(order._2));
    builder.append("] [");
    builder.append(order._1);
    builder.append("] : ");
    println(builder.toString());
  }
}
