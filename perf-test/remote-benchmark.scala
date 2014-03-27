import com.google.common.collect.HashMultiset
import java.io.OutputStreamWriter
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock
import net.minidev.json.{JSONObject, JSONValue}
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
implicit def funToRunnable(fun: () => Unit) = new Runnable() { def run() = fun() }
val toInt = (arg:String) => { try { Some(Integer.parseInt(arg.trim)) } catch { case e: Exception => None }}


val arg = Array[String]("1", "1000", "http://192.168.0.194:9000/order/")
val argMap = Map(0 -> "clientNumber", 1 -> "iterations", 2 -> "url")

for (i <- 0 to args.length - 1) {
  println(s"passed arg ${argMap.get(i).get}: ${args(i)}")
  arg(i) = args(i)
}

val clientsThreadNumber = Option(arg(0)).flatMap(toInt).getOrElse(1)
val latch = new CountDownLatch(clientsThreadNumber)
val iterations = Option(arg(1)).flatMap(toInt).getOrElse(1000)
val url = Option(arg(2)).getOrElse("http://192.168.0.194:9000/order/")

println(s" client thread number: ${clientsThreadNumber} iterations number: ${iterations} url: ${url} ")

val clientCounter = new AtomicInteger()
val lock = new ReentrantLock()

for (i <- 1 to clientsThreadNumber) {
  new Thread(() => {

  val clientId = clientCounter.getAndIncrement
  var fails = Vector[(String, Int)]()
  val latencyHistogram = new ArrayBuffer[Long];
  val threadStatistics = HashMultiset.create[String]()

  println(s" Client №${clientId} started ")
  try {
    for (x <- 1 to iterations) {
      val t0 = System.nanoTime();
      try {
        val content = doRequest(s"${url}${clientId}")
        val t1 = System.nanoTime();
        val timeUs = t1 - t0
        latencyHistogram.append(timeUs)
        threadStatistics.add(content)
      } catch {
        case ioe: java.io.IOException => {
          fails = fails.:+("IOException", clientId)
        }
        case ste: java.net.SocketTimeoutException => {
          fails = fails.:+("SocketTimeoutException", clientId)
        }
      }
    }
  } catch {
    case e: java.lang.Throwable => e.printStackTrace(); latch.countDown
  } finally {
    println(s"client ${clientId} exit ")
  }

  lock.lock
    printStatistic
  lock.unlock
  latch.countDown


  @throws(classOf[java.io.IOException])
  @throws(classOf[java.net.SocketTimeoutException])
  def doRequest(url: String,
                connectTimeout: Int = 1000,
                readTimeout: Int = 1000,
                requestMethod: String = "POST") = {
    import java.net.{URL, HttpURLConnection}
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)

    connection.setDoInput(true)
    connection.setDoOutput(true)

    val outputStream = connection.getOutputStream
    val writer = new OutputStreamWriter(outputStream)

    val json = jsonObject
    JSONValue.writeJSONString(json, writer);
    writer.flush()
    outputStream.flush()
    outputStream.close()

    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close
    content
  }

  def jsonObject = {
    val json = new JSONObject();
    json.put("accountId", Integer.valueOf(clientId))
    json.put("requestId", java.lang.Long.valueOf(System.currentTimeMillis()))
    json.put("sectionId", Integer.valueOf(500))
    json.put("numSeats", Integer.valueOf(2))
    json.put("concertId", Integer.valueOf(321))
    json
  }

  def printStatistic {
    val h = latencyHistogram.sorted
    println(s"*********************${h.length}***************************")
    println(s"Http latency for client №${clientId} : 0, 50%, 90%, 99% Max");

    printf("%d, %d, %d, %d, %d \n",
      h(0),
      h(h.length / 2),
      h((h.length * 0.9).toInt),
      h((h.length * 0.99).toInt),
      h(h.length - 1));

    val it = threadStatistics.elementSet().iterator()
    while (it.hasNext) {
      val currentId = it.next()
      println(s" ${currentId} : ${threadStatistics.count(currentId)}")
    }
    println("*********************************************************")
  }
}).start()
}

latch.await
println("all thread was shutdown")