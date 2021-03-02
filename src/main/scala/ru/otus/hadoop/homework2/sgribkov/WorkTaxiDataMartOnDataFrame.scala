package ru.otus.hadoop.homework2.sgribkov

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object WorkTaxiDataMartOnDataFrame extends ReadData {

  def processTaxiDataDF(factsDF: DataFrame, dimDF: DataFrame): DataFrame = {
    factsDF
      .join(dimDF, col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .count()
      .orderBy(col("count").desc)
      .withColumnRenamed("count", "TripsQty")
  }

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder()
      .appName("WorkTaxiDataMartOnDataFrame")
      .config("spark.master", "local")
      .getOrCreate()


    val taxiZonesDF = readCSV("src/main/resources/data/taxi_zones.csv")
    val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val taxiDataMartDF = processTaxiDataDF(taxiFactsDF, taxiZonesDF)

    taxiDataMartDF.show()

    taxiDataMartDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet("src/main/resources/data/output/taxi_trips_by_boroughs/")
  }

}
