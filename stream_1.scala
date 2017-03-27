def clean_fld(field: String) = Option(field.trim().replaceAll("[^\\w]", "_")).filterNot(_.isEmpty).getOrElse("null")
def clean_str(field: String) = '"' + clean_fld(field) + '"'

stream.foreachRDD { rdd =>
  mul.append("foreach rdd")
  try {
    val parsedList = rdd.reduce(_ ++ _)

    mul.append(s"rdd lists size=${rdd.count}")
    mul.append(s"rdd all rows count size=${parsedList.size}")

    parsedList
      .map(pt => s"vessel_rt,station_id=${pt.MMSI},msg_type=${pt.TYPE}," +
              s"name=${clean_fld(pt.NAME)}," +
              s"dest=${clean_fld(pt.DEST)}," +
              s"arv=${clean_fld(pt.ARV)} " +
              s"lat=${pt.LAT},lon=${pt.LNG},speed=${pt.SOG},heading=0 ${pt.T}000000000")
      .foreach { sline =>
        //mul.append(Http("http://influxdb.maritime-monit.bedccb79.svc.dockerapp.io:8086/write?db=maritime_4").postData(sline).asString.toString)
      }

    val num_vessels = parsedList.groupBy(_.MMSI).size
    val num_vessels_line = s"vessel_count cnt=${num_vessels}"

    mul.append(Http("http://influxdb.maritime-monit.bedccb79.svc.dockerapp.io:8086/write?db=maritime_4").postData(num_vessels_line).asString.toString)
    mul.append(s"num_vessels = ${num_vessels}")

    val vessel_info_line = parsedList.map(p => s"vessel_info name=${clean_str(p.NAME)},dest=${clean_str(p.DEST)}," +
                                        s"arv=${clean_str(p.ARV)},mmsi=${clean_str(p.MMSI)}," +
                                        s"lat=${clean_fld(p.LAT)},lon=${clean_fld(p.LNG)}").distinct
    vessel_info_line.foreach { sline =>
      //mul.append(Http("http://influxdb.maritime-monit.bedccb79.svc.dockerapp.io:8086/write?db=maritime_4").postData(sline).asString.toString)
    }

    mul.append("done")

    } catch {
      case e:Throwable => mul.append(e.toString)
    }
}

ssc.start()
