package org.gateway

import scala.Array
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

case class Route(method: String, path: String*)

/**
 * The companion object
 */
object Reader {
  /**
   * automatically wrap a function in a reader
   */
  implicit def reader[From, To](block: From => To) = Reader[From, To](block)

  /**
   * resolve a reader
   */
  def withDependency[F, T](dep: F)(reader: Reader[F, T]): T = reader(dep)
}

/**
 * The reader Monad
 */
case class Reader[-From, +To](origin: From => To) {

  def apply(c: From) = origin(c)

  def map[ToB](f: To => ToB): Reader[From, ToB] = Reader(c => f(origin(c)))

  def flatMap[FromB <: From, ToB](f: To => Reader[FromB, ToB]): Reader[FromB, ToB] = Reader(c => f(origin(c))(c))
}

object Order {

  def writeInt(i: Int, offset: Int, array: Array[Byte]) {
    array(offset) = (i >>> 24).toByte
    array(offset + 1) = (i >>> 16).toByte
    array(offset + 2) = (i >>> 8).toByte
    array(offset + 3) = i.toByte
  }

  def writeLong(i: Long, offset: Int, array: Array[Byte]) {
    array(offset) = (i >>> 56).toByte
    array(offset + 1) = (i >>> 48).toByte
    array(offset + 2) = (i >>> 40).toByte
    array(offset + 3) = (i >>> 32).toByte
    array(offset + 4) = (i >>> 24).toByte
    array(offset + 5) = (i >>> 16).toByte
    array(offset + 6) = (i >>> 8).toByte
    array(offset + 7) = i.toByte
  }

  /*def unpackInt(offset: Int, bytes: Array[Byte]): Int = {
    ((bytes(offset) << 24 & 0xffffffff) | (bytes(offset + 1) << 16 & 0xffffff)
      | (bytes(offset + 2) << 8 & 0xffff) | (bytes(offset + 3) & 0xff))
  }*/

  def readInt(offset: Int, bytes: Array[Byte]): Int = {
    (bytes(offset) << 24) |
    (bytes(offset + 1) & 0xff) << 16 |
    (bytes(offset + 2) & 0xff) << 8 |
    (bytes(offset + 3) & 0xff)
  }

  def readLong(offset: Int, bytes: Array[Byte]): Long = {
    (bytes(offset) << 56).toLong |
    (bytes(offset + 1) & 0xffL) << 48 |
    (bytes(offset + 2) & 0xffL) << 40 |
    (bytes(offset + 3) & 0xffL) << 32 |
    (bytes(offset + 4) & 0xffL) << 24 |
    (bytes(offset + 5) & 0xffL) << 16 |
    (bytes(offset + 6) & 0xffL) << 8  |
    (bytes(offset + 7) & 0xffL)
  }

  def decode(bytes: Array[Byte]): Order = {
    new Order(readInt(0, bytes), readLong(4, bytes),
      readInt(12, bytes), readInt(16, bytes), readInt(20, bytes))
  }

  def encode(order: Order): Array[Byte] = {
    val array = new Array[Byte](24)
    writeInt(order.accountId, 0, array)
    writeLong(order.requestId, 4, array)
    writeInt(order.sectionId, 12, array)
    writeInt(order.numSeats, 16, array)
    writeInt(order.concertId, 20, array)
    array
  }
}

case class Order(
  val accountId: Int,
  val requestId: Long,
  val sectionId: Int,
  val numSeats: Int,
  val concertId: Int)


object RequestHandler {

  private def parsePath(pathStr: String) = pathStr.trim.split('/').filter(_.length > 0)

  def parseRoute(method: String, pathStr: String) = Route(method, parsePath(pathStr): _*)
}

object Routes {

  val router: PartialFunction[Route, Boolean] = {
    //case Route("GET", "order", clientId) => true
    case Route("POST", "order", clientId) => true
  }

  val fail: PartialFunction[Route, Boolean] = {
    new PartialFunction[Route, Boolean] {
      def apply(v1: Route): Boolean = {
        false
      }

      def isDefinedAt(x: Route): Boolean = true
    }
  }
}

class NamedThreadFactory(var namePrefix: String) extends ThreadFactory {
  val POOL_NUMBER = new AtomicInteger(1)
  val group: ThreadGroup = Thread.currentThread().getThreadGroup()
  val threadNumber = new AtomicInteger(1)

  {
    namePrefix = namePrefix + "(pool" + POOL_NUMBER.getAndIncrement() + "-thread-"
  }

  def newThread(r: Runnable) = {
    new Thread(this.group, r, namePrefix + this.threadNumber.getAndIncrement() + ")", 0L)
  }
}
