package org.gateway

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import java.io.OutputStreamWriter
import net.minidev.json.{JSONObject, JSONValue}
import scala.concurrent._
import scala.language.postfixOps
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Try, Success}
import scala.async.Async._
import scala.collection.mutable.ListBuffer
import scala.util.Failure
import scala.util.Success
import scala.annotation.tailrec

@RunWith(classOf[JUnitRunner])
class FuturesTests extends FunSuite with ShouldMatchers  {

  @throws(classOf[java.io.IOException])
  @throws(classOf[java.net.SocketTimeoutException])
  private def request(url: String,
              clientId: Int,
              connectTimeout: Int = 1000,
              readTimeout: Int = 1000,
              requestMethod: String = "POST") = {
    println(s"in request thread: ${Thread.currentThread()} ${clientId}")
    import java.net.{URL, HttpURLConnection}
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)

    connection.setDoInput(true)
    connection.setDoOutput(true)

    val outputStream = connection.getOutputStream
    val writer = new OutputStreamWriter(outputStream)

    val json = jsonObject(clientId)
    JSONValue.writeJSONString(json, writer);
    writer.flush()
    outputStream.flush()
    outputStream.close()

    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close
    content
  }

  def jsonObject(clientId: Int) = {
    val json = new JSONObject();
    json.put("accountId", Integer.valueOf(clientId))
    json.put("requestId", java.lang.Long.valueOf(System.currentTimeMillis()))
    json.put("sectionId", Integer.valueOf(500))
    json.put("numSeats", Integer.valueOf(2))
    json.put("concertId", Integer.valueOf(321))
    json
  }

  object HttpClient {
    def apply(url: String, clientId: Int): Future[String] = {
      future { println("call begin"); request(url + clientId, clientId) }
    }
  }

  object TryDelayedHttpClient {
    def apply(url: String, clientId: Int, secTryDelay: Int): Future[Try[String]] = {
      future { Thread sleep secTryDelay * 1000; Try(request(url + clientId, clientId)) }
    }
  }


  class AsyncAction[T](num: Int, block: => Future[T], secTryDelay: Int) extends (() => Future[T]) {
    def apply() = {
      Thread sleep(secTryDelay * 1000); println(s"attempt ${num} secTryDelay:${secTryDelay}" ); block
    }
  }

  implicit class FallbackFuture[T](generalAction: Future[T]) {

    def withFallbackStrategy(fallBackAction: => Future[T]): Future[T] = {
      generalAction recoverWith {
        case _ => fallBackAction recoverWith { case _ => generalAction }
      }
    }
  }

  /**
   * Retry with foldRight
   *
   * @param noTimes
   * @param secTryDelay
   * @param block
   * @return
   */

  def retry[T](noTimes: Int, secTryDelay: Int) (block: => Future[T]): () => Future[T] = {
    val attempts: Seq[AsyncAction[T]] = (1 to noTimes) map ( i => new AsyncAction[T](i, block, i * 2/3) )

    println("Max attempts: " + attempts.size)
    val failed = () => Future.failed[T](new Exception)

    val fold: ( () => Future[T], () => Future[T] ) => (() => Future[T]) =
      ((block: () => Future[T], acc: () => Future[T]) => () => { block() withFallbackStrategy { acc() } })

    val res = attempts.foldRight(failed) { fold }

    println("fold done")
    res
  }

  /**
   * Retry with async
   * imperative way with async
   */
  def retryWithAsync[T](noTimes: Int) (block: => Future[Try[T]]): Future[T] = async {
    var iterations = 0
    var result: Try[T] = Failure(new Exception("Error !!!"))
    while (iterations < noTimes && result.isFailure) {
      println(s"attempt: ${iterations}")
      result = await { block }
      iterations += 1
    }

    result get
  }

  /*test("Test future retry with foldRight") {
    val secTryInterval = 60
    val secTryDelay = 5

    val clientId = 1
        
    val tryNumber = secTryInterval/secTryDelay 

    val futureFunc = retry(tryNumber, secTryDelay) { HttpClient("http://192.168.0.194:9000/order/", clientId) }

    //after this all functions begin evaluation
    val f = futureFunc()

    f onComplete {
      case Success(r) => println("on Success:" + r)
      case _ =>
    }

    f onFailure {
      case ex => println("on Failure: " + ex)
    }

    Await.result(f, secTryInterval seconds)
  }*/

  /*
  test(" retry with Async ") {
    val secTryInterval = 60
    val secTryDelay = 5

    val clientId = 1
    val tryNumber = secTryInterval/secTryDelay

    val f = retryWithAsync(tryNumber) { TryHttpClient("http://192.168.0.194:9000/order/", secTryDelay, clientId) }

    f onComplete {
      case Success(r) => println("on Success:" + r)
      case _ =>
    }

    f onFailure {
      case ex => println("on Failure: " + ex)
    }

    Await.result(f, secTryInterval seconds)
  }
  */

  object TryHttpClient {
    def apply(url: String, secTryDelay: Int, clientId: Int): Future[Try[String]] = {
      future { Thread sleep(secTryDelay * 1000); Try(request(url + clientId, clientId)) }
    }

    def apply(url: String, clientId: Int): Future[Try[String]] = {
      future { Try(request(url + clientId, clientId)) }
    }
  }

  def toSequence(seq: List[Future[Try[String]]]): Future[List[Try[String]]] = async {
    var tseq = seq
    var r = ListBuffer[Try[String]]()

    while (tseq != Nil) {
      r += await { tseq.head }
      tseq = tseq.tail
    }

    r.result
  }

  @tailrec
  private def iterateOver(res0: List[Try[String]]): Unit = res0 match {
    case Nil => println("All result was printed")
    case head :: tail => {
      head match {
        case Success(elem) => println(elem)
        case Failure(ex) => println(s"Failure ${ex.getMessage}")
      }
      iterateOver(tail)
    }
  }

  /*test("3. Scala sequence of futures with await ") {

    var sequence: List[Future[Try[String]]] = Nil

    (1 to 10) map { i => sequence = List(TryHttpClient("http://192.168.0.194:9000/order/", i)) ++ sequence }

    val aggregateFuture = toSequence(sequence)
    aggregateFuture onComplete { case Success(res) => println("Received results"); iterateOver(res) }

    Await result(aggregateFuture , 10 second)
  }

  test("4. Scala sequence of futures with foldRight") {
    val startFuture: Future[List[Try[String]]] = Future.successful(List[Try[String]]())

    val sequence: Seq[Future[Try[String]]] = (1 to 50) map { i => TryHttpClient("http://192.168.0.194:9000/order/", i) }

    val aggregateFuture = sequence.foldRight(startFuture) {
      (currentFuture: Future[Try[String]], acc: Future[List[Try[String]]]) => for { x <- currentFuture; xs <- acc} yield x :: xs
    }

    aggregateFuture onComplete { case Success(res) => println("Received results"); iterateOver(res) }

    Await result(aggregateFuture , 10 second)
  }*/

}