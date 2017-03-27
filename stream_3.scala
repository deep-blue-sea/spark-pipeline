Spark Notebook Spark NotebookStreamingToInflux (unsaved changes)
Scala [2.11.8] Spark [2.1.0] Hadoop [2.7.3]
File
Edit
View
Insert
Cell
Kernel
Help
Cell Toolbar:

:dp org.apache.spark %% spark-streaming % 2.1.0
globalScope.jars: Array[String] = [Ljava.lang.String;@dc3f6a6
res2: List[String] = List(/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/apache/avro/avro/1.7.7/avro-1.7.7.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/apache/xbean/xbean-asm5-shaded/4.4/xbean-asm5-shaded-4.4.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/commons-digester/commons-digester/1.8/commons-digester-1.8.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/tukaani/xz/1.0/xz-1.0.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/com/fasterxml/jackson/module/jackson-module-paranamer/2.6.5/jackson-module-paranamer-2.6.5.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/com/google/protobuf/proto...
120 entries total
Search: Show:
string value
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/apache/avro/avro/1.7.7/avro-1.7.7.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/apache/xbean/xbean-asm5-shaded/4.4/xbean-asm5-shaded-4.4.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/commons-digester/commons-digester/1.8/commons-digester-1.8.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/tukaani/xz/1.0/xz-1.0.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/com/fasterxml/jackson/module/jackson-module-paranamer/2.6.5/jackson-module-paranamer-2.6.5.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/javax/annotation/javax.annotation-api/1.2/javax.annotation-api-1.2.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/apache/avro/avro-ipc/1.7.7/avro-ipc-1.7.7-tests.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/apache/hadoop/hadoop-auth/2.2.0/hadoop-auth-2.2.0.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/codehaus/jackson/jackson-core-asl/1.9.13/jackson-core-asl-1.9.13.jar
Pages:
Previous
1
2
Next
Showing 1 to 10 of 11 records

Took: 4 seconds 852 milliseconds, at 2017-3-25 23:54

:dp net.liftweb %% lift-json % 2.6.3
globalScope.jars: Array[String] = [Ljava.lang.String;@1857674b
res4: List[String] = List(/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-reflect/2.11.1/scala-reflect-2.11.1.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-library/2.11.1/scala-library-2.11.1.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/net/liftweb/lift-json_2.11/2.6.3/lift-json_2.11-2.6.3.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/com/thoughtworks/paranamer/paranamer/2.4.1/paranamer-2.4.1.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/modules/scala-parser-combinators_2.11/1.0.1/scala-parser-combinators_2.11-1.0.1.jar, /tmp/spark-notebook/aether/1849483c-1a40-...
127 entries total
Search: Show:
string value
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-reflect/2.11.1/scala-reflect-2.11.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-library/2.11.1/scala-library-2.11.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/net/liftweb/lift-json_2.11/2.6.3/lift-json_2.11-2.6.3.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/com/thoughtworks/paranamer/paranamer/2.4.1/paranamer-2.4.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/modules/scala-parser-combinators_2.11/1.0.1/scala-parser-combinators_2.11-1.0.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-compiler/2.11.1/scala-compiler-2.11.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scalap/2.11.1/scalap-2.11.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/modules/scala-xml_2.11/1.0.2/scala-xml_2.11-1.0.2.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/apache/avro/avro/1.7.7/avro-1.7.7.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/apache/xbean/xbean-asm5-shaded/4.4/xbean-asm5-shaded-4.4.jar
Pages:
Previous
1
2
3
…
13
Next
Showing 127 of 127 records

Took: 3 seconds 670 milliseconds, at 2017-3-25 23:54

:dp org.scalaj %% scalaj-http % 2.3.0
globalScope.jars: Array[String] = [Ljava.lang.String;@2b10cd34
res4: List[String] = List(/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scalaj/scalaj-http_2.11/2.3.0/scalaj-http_2.11-2.3.0.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-library/2.11.7/scala-library-2.11.7.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-reflect/2.11.1/scala-reflect-2.11.1.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-library/2.11.1/scala-library-2.11.1.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/net/liftweb/lift-json_2.11/2.6.3/lift-json_2.11-2.6.3.jar, /tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/com/thoughtworks/...
11 entries total
Search: Show:
string value
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scalaj/scalaj-http_2.11/2.3.0/scalaj-http_2.11-2.3.0.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-library/2.11.7/scala-library-2.11.7.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-reflect/2.11.1/scala-reflect-2.11.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-library/2.11.1/scala-library-2.11.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/net/liftweb/lift-json_2.11/2.6.3/lift-json_2.11-2.6.3.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/com/thoughtworks/paranamer/paranamer/2.4.1/paranamer-2.4.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/modules/scala-parser-combinators_2.11/1.0.1/scala-parser-combinators_2.11-1.0.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scala-compiler/2.11.1/scala-compiler-2.11.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/scalap/2.11.1/scalap-2.11.1.jar
/tmp/spark-notebook/aether/1849483c-1a40-41eb-af14-2134bb98c4af/org/scala-lang/modules/scala-xml_2.11/1.0.2/scala-xml_2.11-1.0.2.jar
Pages:
Previous
1
2
Next
Showing 11 of 11 records

Took: 3 seconds 226 milliseconds, at 2017-3-25 23:55

import org.apache.spark.ui.notebook.front.widgets.SparkInfo
import scala.concurrent.duration._
import scala.language.postfixOps
new SparkInfo(sparkContext, checkInterval=1 seconds, execNumber=Some(100))
import org.apache.spark.ui.notebook.front.widgets.SparkInfo
import scala.concurrent.duration._
import scala.language.postfixOps
res6: org.apache.spark.ui.notebook.front.widgets.SparkInfo = <SparkInfo widget>
Total Duration: 25987
Scheduling Mode: Unknown
Active Stages: 0
Completed Stages: 0
Failed Stages: 0
Active Stages
Completed Stages
Took: 3 seconds 249 milliseconds, at 2017-3-25 23:55

import org.apache.spark.streaming.{StreamingContext, Duration}
val streaming = new StreamingContext(sparkContext, Duration(5000))
import org.apache.spark.streaming.{StreamingContext, Duration}
streaming: org.apache.spark.streaming.StreamingContext = org.apache.spark.streaming.StreamingContext@39eb287f
Took: 2 seconds 257 milliseconds, at 2017-3-25 23:55

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import scalaj.http.Http
​
class CustomReceiver(url: String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
​
  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }
​
  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }
​
  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    try {
      while(!isStopped) {
        store(Http(url).asString.body)
      }

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + url, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import scalaj.http.Http
defined class CustomReceiver
Took: 1 second 686 milliseconds, at 2017-3-25 23:55

val ssc = streaming
val stream = ssc.receiverStream(new CustomReceiver("http://www.myshiptracking.com/requests/vesselsonmap.php?type=json&minlat=-36.77678660818592" +
                        "&maxlat=74.00651358074181&minlon=-147.33959937500003&maxlon=106.66430687499997&zoom=3&mmsi=null"))
ssc: org.apache.spark.streaming.StreamingContext = org.apache.spark.streaming.StreamingContext@39eb287f
stream: org.apache.spark.streaming.dstream.ReceiverInputDStream[String] = org.apache.spark.streaming.dstream.PluggableInputDStream@7d49da6d
Took: 2 seconds 509 milliseconds, at 2017-3-25 23:55

import java.io._
import scala.collection.mutable._
import java.util.Properties
import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import net.liftweb.json._
implicit val formats = DefaultFormats
​
case class MaritimePoint(
                          MMSI: String,
                          NAME: String,
                          SOG: String,
                          COG: String,
                          TYPE: String,
                          T: String,
                          R: String,
                          LAT: String,
                          LNG: String,
                          S1: String,
                          S2: String,
                          S3: String,
                          S4: String,
                          IMO: String,
                          DEST: String,
                          ARV: String
                        )
​
case class MatPoWrapper( TIME: String, DATA: MaritimePoint )
import java.io._
import scala.collection.mutable._
import java.util.Properties
import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import net.liftweb.json._
formats: net.liftweb.json.DefaultFormats.type = net.liftweb.json.DefaultFormats$@2636468b
defined class MaritimePoint
defined class MatPoWrapper
Took: 3 seconds 45 milliseconds, at 2017-3-25 23:55

val logs = ul()
logs
logs: notebook.front.widgets.HtmlList = <HtmlList widget>
res12: notebook.front.widgets.HtmlList = <HtmlList widget>
HttpResponse({"error":"unable to parse 'vessel_rt,station_id=538006273,msg_type=7,name=ELSA,dest=SN_DKR,arv= lat=8.2024,lon=-17.06668,speed=12.2,heading=0 1490486127000000000': missing tag value"} ,400,Map(Content-Encoding -> Vector(gzip), Content-Length -> Vector(183), Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:55:50 GMT), Request-Id -> Vector(933277fd-11b6-11e7-99af-000000000000), Status -> Vector(HTTP/1.1 400 Bad Request), X-Influxdb-Version -> Vector(1.2.2)))
HttpResponse({"error":"unable to parse 'vessel_rt,station_id=563012500,msg_type=7,name=LOWLANDS_HOPE,dest=LAS_PALMAS,arv= lat=8.06654,lon=-15.48362,speed=11.3,heading=0 1490486103000000000': missing tag value"} ,400,Map(Content-Encoding -> Vector(gzip), Content-Length -> Vector(196), Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:55:50 GMT), Request-Id -> Vector(9332facd-11b6-11e7-99b0-000000000000), Status -> Vector(HTTP/1.1 400 Bad Request), X-Influxdb-Version -> Vector(1.2.2)))
HttpResponse({"error":"unable to parse 'vessel_rt,station_id=244690348,msg_type=0,name=CHALLENGE,dest=KRIMPEN_AAN_DE_LEK,arv= lat=5.67987,lon=5.3522,speed=2.4,heading=0 1490485990000000000': missing tag value"} ,400,Map(Content-Encoding -> Vector(gzip), Content-Length -> Vector(195), Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:55:50 GMT), Request-Id -> Vector(933380ab-11b6-11e7-99b1-000000000000), Status -> Vector(HTTP/1.1 400 Bad Request), X-Influxdb-Version -> Vector(1.2.2)))
HttpResponse({"error":"unable to parse 'vessel_rt,station_id=5250012,msg_type=11,name=,dest=,arv= lat=1.18117,lon=104.0145,speed=0,heading=0 1490486118000000000': missing tag value"} ,400,Map(Content-Encoding -> Vector(gzip), Content-Length -> Vector(161), Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:55:50 GMT), Request-Id -> Vector(93340ea2-11b6-11e7-99b2-000000000000), Status -> Vector(HTTP/1.1 400 Bad Request), X-Influxdb-Version -> Vector(1.2.2)))
HttpResponse({"error":"unable to parse 'vessel_rt,station_id=358580494,msg_type=13,name=G=,dest=,arv= lat=12.34771,lon=112.03943,speed=0,heading=0 1490482146000000000': invalid tag format"} ,400,Map(Content-Encoding -> Vector(gzip), Content-Length -> Vector(168), Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:55:50 GMT), Request-Id -> Vector(9334cbec-11b6-11e7-99b3-000000000000), Status -> Vector(HTTP/1.1 400 Bad Request), X-Influxdb-Version -> Vector(1.2.2)))
HttpResponse({"error":"unable to parse 'vessel_rt,station_id=412767090,msg_type=7,name=XIE_HAI_ZHI_HUI,dest=ZHANJIANG,arv= lat=21.07838,lon=110.55865,speed=0.4,heading=0 1490486018000000000': missing tag value"} ,400,Map(Content-Encoding -> Vector(gzip), Content-Length -> Vector(196), Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:55:50 GMT), Request-Id -> Vector(933586fe-11b6-11e7-99b4-000000000000), Status -> Vector(HTTP/1.1 400 Bad Request), X-Influxdb-Version -> Vector(1.2.2)))
HttpResponse({"error":"unable to parse 'vessel_rt,station_id=5330506,msg_type=11,name=,dest=,arv= lat=5.42659,lon=100.25206,speed=0,heading=0 1490486098000000000': missing tag value"} ,400,Map(Content-Encoding -> Vector(gzip), Content-Length -> Vector(163), Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:55:50 GMT), Request-Id -> Vector(93362daf-11b6-11e7-99b5-000000000000), Status -> Vector(HTTP/1.1 400 Bad Request), X-Influxdb-Version -> Vector(1.2.2)))
HttpResponse({"error":"unable to parse 'vessel_rt,station_id=995330048,msg_type=13,name=TJ9_WRECK,dest=,arv= lat=2.71612,lon=101.27712,speed=0,heading=0 1490486133000000000': missing tag value"} ,400,Map(Content-Encoding -> Vector(gzip), Content-Length -> Vector(182), Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:55:50 GMT), Request-Id -> Vector(933b19ad-11b6-11e7-99b6-000000000000), Status -> Vector(HTTP/1.1 400 Bad Request), X-Influxdb-Version -> Vector(1.2.2)))
HttpResponse({"error":"unable to parse 'vessel_rt,station_id=538006609,msg_type=8,name=FRONT_CHEETAH,dest=FUJAIRAH,UAE,arv= lat=1.77913,lon=102.63915,speed=11.5,heading=0 1490486081000000000': missing tag value"} ,400,Map(Content-Encoding -> Vector(gzip), Content-Length -> Vector(197), Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:55:51 GMT), Request-Id -> Vector(933cf45f-11b6-11e7-99b7-000000000000), Status -> Vector(HTTP/1.1 400 Bad Request), X-Influxdb-Version -> Vector(1.2.2)))
HttpResponse({"error":"unable to parse 'vessel_rt,station_id=415963782,msg_type=10,name=LIAOYINGYU35730,dest=,arv= lat=.00601,lon=121.06395,speed=1.1,heading=0 1490485455000000000': missing tag value"} ,400,Map(Content-Encoding -> Vector(gzip), Content-Length -> Vector(187), Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:55:51 GMT), Request-Id -> Vector(933d775c-11b6-11e7-99b8-000000000000), Status -> Vector(HTTP/1.1 400 Bad Request), X-Influxdb-Version -> Vector(1.2.2)))
Took: 3 seconds 359 milliseconds, at 2017-3-25 23:55

stream.foreachRDD {
  rdd =>
  logs.append(s"foreach rdd len=${rdd.count}")
       try {
         logs.append("trying")
  val parsed_pts = rdd
    .collect
    .flatMap(str => parse(str).children.head.\\("DATA").extract[List[MaritimePoint]])
    .map(pt => s"vessel_rt,station_id=${pt.MMSI},msg_type=${pt.TYPE},name=${pt.NAME.replace(" ", "_")},dest=${pt.DEST.replace(" ", "_")},arv=${pt.ARV.replace(" ", "_")} " +
                      s"lat=${pt.LAT},lon=${pt.LNG},speed=${pt.SOG},heading=0 ${pt.T}000000000")


  logs.append("parsed")
   logs.append(parsed_pts.take(3).toList.toString)
                  logs.append("parsed logged")


   parsed_pts.foreach { sline =>
     logs.append(Http("http://influxdb.maritime-monit.bedccb79.svc.dockerapp.io:8086/write?db=maritime_4").postData(sline).asString.toString)
   }}
                   catch {
                     case e:Throwable => logs.append(e.toString)
                   }
}
streaming.start()
Took: 8 seconds 309 milliseconds, at 2017-3-25 23:55

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
Future { Thread.sleep(60000) ; streaming.stop() }
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
res18: scala.concurrent.Future[Unit] = List()
cancel

Http("http://influxdb.maritime-monit.bedccb79.svc.dockerapp.io:8086/write?db=maritime_4")
  .postData("cpu,host=server02,region=uswest load=78 1434055562000000000").asString.toString
res18: String = HttpResponse(,204,Map(Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:45:29 GMT), Request-Id -> Vector(2099794b-11b5-11e7-bbaa-000000000000), Status -> Vector(HTTP/1.1 204 No Content), X-Influxdb-Version -> Vector(1.2.2)))
HttpResponse(,204,Map(Content-Type -> Vector(application/json), Date -> Vector(Sat, 25 Mar 2017 23:45:29 GMT), Request-Id -> Vector(2099794b-11b5-11e7-bbaa-000000000000), Status -> Vector(HTTP/1.1 204 No Content), X-Influxdb-Version -> Vector(1.2.2)))Took: 4 seconds 935 milliseconds, at 2017-3-25 23:45

"tes te stt".replace(" ", "_")
res14: String = tes_te_stt
tes_te_sttTook: 3 seconds 927 milliseconds, at 2017-3-25 23:55

​
Build: |  buildTime-Thu Feb 02 05:18:02 UTC 2017 | formattedShaVersion-0.7.0-c955e71d0204599035f603109527e679aa3bd570 | sbtVersion-0.13.8 | scalaVersion-2.11.8 | sparkNotebookVersion-0.7.0 | hadoopVersion-2.7.3 | jets3tVersion-0.7.1 | jlineDef-(jline,2.12) | sparkVersion-2.1.0 | withHive-false |.
