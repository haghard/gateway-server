package org.gateway

import net.minidev.json.JSONObject

object Extensions {

  implicit def toInt = (arg:String) => { try { Some(Integer.parseInt(arg.trim)) } catch { case e: Exception => None }}

  implicit def string2Option(s: String) : Option[String] = Some(s)

  implicit def funToRunnable(fun: () => Unit) = new Runnable() { def run() = fun() }

  implicit def json2Domain(jo: JSONObject) = {
    new Order(jo.get("accountId").asInstanceOf[Int],
      jo.get("requestId").asInstanceOf[Long],
      jo.get("sectionId").asInstanceOf[Int],
      jo.get("numSeats").asInstanceOf[Int],
      jo.get("concertId").asInstanceOf[Int])
  }

}
