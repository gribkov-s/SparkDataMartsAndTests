package ru.otus.hadoop.homework2.sgribkov

import org.apache.spark.sql.{DataFrame, SparkSession}

trait ReadData {

  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)

  def readCSV(path: String)(implicit spark: SparkSession):DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

}
