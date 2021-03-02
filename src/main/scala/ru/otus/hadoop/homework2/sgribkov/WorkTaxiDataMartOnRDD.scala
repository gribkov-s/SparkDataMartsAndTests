package ru.otus.hadoop.homework2.sgribkov

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object WorkTaxiDataMartOnRDD extends ReadData {

  val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def processTaxiDataRDD(factsDS: RDD[TaxiTrip]): RDD[(Int, Int)] = {
  factsDS
    .keyBy(r => LocalDateTime.parse(r.tpep_pickup_datetime, dtf).getHour)
    .mapValues(_ => 1)
    .reduceByKey(_ +_)
    .sortBy(r => r._2, false)
  }

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder()
      .appName("WorkTaxiDataMartOnRDD")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val taxiFactsRDD:  RDD[TaxiTrip] = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
      .as[TaxiTrip]
      .rdd

    val taxiDataMartRDD = processTaxiDataRDD(taxiFactsRDD)

    taxiDataMartRDD.foreach(println)

    taxiDataMartRDD
      .map { case (k, v) => Array(k, v).mkString(" ") }
      .coalesce(1)
      .saveAsTextFile("src/main/resources/data/output/taxi_calls_by_hours/")
  }

}
