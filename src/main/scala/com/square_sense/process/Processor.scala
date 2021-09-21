package com.square_sense.process

import com.square_sense.models.EECMeter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Processor {
  import org.apache.spark.sql.SparkSession

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Electrical Energy Consumption Processor")
      .config("spark.master", "local[*]")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // For implicit conversions
  import spark.implicits._

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  /** Main function */
  def main(args: Array[String]): Unit = {

    val finalDf = process("data")

    finalDf.printSchema()
    //finalDf.show(false)

    finalDf.write
      .option("header", "true")
      .option("sep", ",")
      .csv(s"output")

  }

  def process(path:String):DataFrame = {
    //read data from csv files
    val rawDf = readDf(path)

    //parse time from string to date
    val formattedDf = parseDate(rawDf)

    val metricsDf = computeMetrics(formattedDf)

    metricsDf
  }

  def readDf(path:String):DataFrame = {
    spark.read
      .option("header", "true")
      .schema(EECMeter.schema)
      .option("sep", ",")
      .csv(s"${path}/*")
  }

  def parseDate(df:DataFrame):DataFrame =
    df.withColumn("time", $"time".cast("timestamp"))

  def computeMetrics(df:DataFrame):DataFrame = {

    val medianUDF = udf((array:Seq[Double])=>array.sortWith(_ < _).drop(array.length/2).head) //same principle for other quantiles

    df
      .groupBy($"meter", window($"time", "1 hour"))
      .agg(first($"time") as "start_time",
        last($"time") as "end_time",
        (last($"energy")-first($"energy")) as "energy_consumption",
        mean($"power") as "mean_power",
        min($"power") as "min_power",
        max($"power") as "max_power",
        stddev($"power") as "stddev_power",
        collect_list($"power") as "powers")
      .withColumn("median_power", medianUDF($"powers"))
      .drop("window", "powers")
  }

  }
