package org.gateway

import io.netty.channel._
import io.netty.handler.codec.http._
import org.slf4j.LoggerFactory
import io.netty.buffer.{Unpooled, ByteBuf}
import scala.PartialFunction
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioDatagramChannel
import java.net.InetSocketAddress
import net.minidev.json.JSONObject
import io.netty.handler.codec.http.HttpVersion._
import io.netty.handler.codec.http.HttpResponseStatus._
import net.minidev.json.parser.JSONParser
import io.netty.channel.ChannelHandler.Sharable
import scala.Some
import Reader._
import RequestHandler._

@Sharable
class RoutingHandler(router: PartialFunction[Route, Boolean])
  extends SimpleChannelInboundHandler[DefaultHttpRequest](classOf[DefaultHttpRequest]) {

  private val bootstrap = new Bootstrap()
  private val group = new NioEventLoopGroup()
  private val outboundChannel = broadcastOutboundChannel
  //for multithreaded cases
  private val parser = new ThreadLocal[JSONParser]() {
    override def initialValue(): JSONParser = {
      new JSONParser(JSONParser.MODE_RFC4627)
    }
  }

  private def logger = LoggerFactory.getLogger(classOf[RoutingHandler])

  private def broadcastOutboundChannel() = {
    //not sure about synchronized, but since it's @Sharable
    this.synchronized {
      bootstrap.group(group)
        .channel(classOf[NioDatagramChannel])
        .option(ChannelOption.SO_BROADCAST, java.lang.Boolean.TRUE)
        .handler(new Encoder(new InetSocketAddress("255.255.255.255", 9090)))
      bootstrap.bind(0).sync().channel()
    }
  }

  def createResponse(order: Option[Order]): Reader[DefaultFullHttpRequest, DefaultFullHttpResponse] = {
    reader {
      (req: DefaultFullHttpRequest) => {
        order match {
          case Some(_) => {
            val response = new DefaultFullHttpResponse(HTTP_1_1, OK)
            val body = s"${Thread.currentThread()}".getBytes
            response.content().writeBytes(body)
            response.headers.add("Content-Type", "text/plain")
            response.headers.add("Content-Length", body.size.toString)
            response
          }
          case None => {
            val response = new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST)
            val body = s"${Thread.currentThread()} [this kind of request are denied]".getBytes
            response.content().writeBytes(body)
            response.headers.add("Content-Type", "text/plain")
            response.headers.add("Content-Length", body.size.toString)
            response
          }
        }
      }
    }
  }

  def broadcast(order: Option[Order]): Reader[DefaultFullHttpRequest, Option[Order]] = {
    reader { req: DefaultFullHttpRequest => { if (order.isDefined) outboundChannel.writeAndFlush(order.get); order } }
  }

  import Extensions._
  def domainObject(json: Option[JSONObject]): Reader[DefaultFullHttpRequest, Option[Order]] = {
    json match {
      case Some(j) => reader { (req: DefaultFullHttpRequest) => Some(j) }
      case None => reader { (req: DefaultFullHttpRequest) => None }
    }
  }

  def jsonObject(buf: ByteBuf): Reader[DefaultFullHttpRequest, Option[JSONObject]] = reader {
    req => {
      val route = parseRoute(req.getMethod.name, req.getUri)
      if (router(route)) {
        buf match {
          case direct if (!direct.hasArray) => {
            try {
              val array = new Array[Byte](direct.readableBytes)
              direct.getBytes(0, array)
              val jsonObject = parser.get.parse(array).asInstanceOf[JSONObject]
              Some(jsonObject)
            } catch {
              case pex: net.minidev.json.parser.ParseException => {
                logger.debug("ParseException: " + pex.getMessage)
                None
              }
              case ex: Throwable => {
                logger.debug("Throwable :" + ex.getMessage)
                None
              }
            }
          }
        }
      } else {
        //unsupported url
        None
      }
    }
  }

  def reply(byteBuf: ByteBuf): Reader[DefaultFullHttpRequest, DefaultFullHttpResponse] = {
    for {
      json <- jsonObject(byteBuf)
      domain <- domainObject(json)
      broadcastedDomain <- broadcast(domain)
      resp <- createResponse(broadcastedDomain)
    } yield resp
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: DefaultHttpRequest) {
    req match {
      case request: DefaultFullHttpRequest => {
        ctx.fireChannelRead(reply(request content)(request))
      }
      case _ => logger.debug("invalid http request")
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    logger.debug("RoutingHandler.Exception: " + cause.getMessage)
    ctx.close()
  }

  def shutdownBroadcast {
    logger.debug("close broadcast")
    if (outboundChannel.isActive) {
      outboundChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
    group.shutdownGracefully().sync()
  }
}