package com.square_sense.models

import org.apache.spark.sql.types._



object EECMeter {
  val schema: StructType = StructType(
    List(
      StructField("meter", StringType, nullable = false),
      StructField("time", StringType, nullable = false),
      StructField("energy", DoubleType, nullable = false),
      StructField("power", DoubleType, nullable = false)
    )
  )
}
