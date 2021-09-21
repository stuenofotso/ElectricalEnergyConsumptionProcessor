package com.square_sense

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.scalatest.FlatSpec

class ProcessorSpec extends FlatSpec  {


  val testDataPath: String = getClass.getResource("/data").getPath


  "Processor.readDf" should "return a valid dataframe" in {

    val df = process.Processor.readDf(testDataPath)
    df.printSchema()
    assert(df.schema == models.EECMeter.schema)
    assert(df.count()==10)
  }

  "Processor.parseDate" should "return a dataframe with time casted to timestamp" in {

    process.Processor.spark.conf.set("spark.sql.session.timeZone", "UTC")

    val data = Seq(("A", "2019-06-01 14:00", 600, 300), ("B", "2019-06-01 10:00", 600, 300))
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    formatter.setTimeZone(TimeZone.getTimeZone("GMT"))

    val df = process.Processor.spark.createDataFrame(data).toDF("meter","time","energy","power")

    val out = process.Processor.parseDate(df)
    out.printSchema()

    assert(out.count()==df.count())

    val res = out.orderBy("meter").collect()
    assert(res(0).get(1).isInstanceOf[Timestamp])
    assert(formatter.format(res(0).getAs[Timestamp](1)) == "2019-06-01 14:00")
  }

  "Processor.process" should "process csv files and compute metrics" in {

    val df = process.Processor.process(testDataPath)
    df.printSchema()
    assert(df.columns.sameElements("meter,start_time,end_time,energy_consumption,mean_power,min_power,max_power,stddev_power,median_power".split(",")))
    assert(df.count()==4)
  }

}
