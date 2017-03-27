import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import java.io._
import scala.collection.mutable._
import java.util.Properties
import scala.concurrent.duration._
import scala.language.postfixOps

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

class CustomReceiver(url: String)
  extends Receiver[List[MaritimePoint]](StorageLevel.MEMORY_ONLY) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {}

  private def receive() {
    while(!isStopped) {
      import net.liftweb.json._
      implicit val formats = net.liftweb.json.DefaultFormats
      println("getting json maritime data")
      val body = scalaj.http.Http(url).asString.body
      println("parsing json maritime data")
      val parsed = parse(body).children.head.\\("DATA").extract[List[MaritimePoint]]
      println(s"storing list cnt=${parsed.size}")
      store(parsed)
      println("waiting...")
      Thread.sleep(2500)
    }
  }
}

val ssc = new StreamingContext(sparkContext, Duration(2000))
val stream = ssc.receiverStream(new CustomReceiver("http://www.myshiptracking.com/requests/vesselsonmap.php?type=json&minlat=-36.77678660818592" +
                        "&maxlat=74.00651358074181&minlon=-147.33959937500003&maxlon=106.66430687499997&zoom=3&mmsi=null"))
