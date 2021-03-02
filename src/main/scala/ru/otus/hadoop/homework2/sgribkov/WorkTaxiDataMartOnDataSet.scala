package ru.otus.hadoop.homework2.sgribkov

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object WorkTaxiDataMartOnDataSet extends ReadData {

def processTaxiDataDS(factsDS: Dataset[TaxiTrip]): DataFrame = {

  val distanceGroupLower = floor(factsDS.col("trip_distance") / 5) * 5

  val tripTime = (
    unix_timestamp(factsDS.col("tpep_dropoff_datetime")) -
    unix_timestamp(factsDS.col("tpep_pickup_datetime"))
    ) / 60

  factsDS
    .filter(r => r.trip_distance != 0 && r.total_amount > 0)
    .withColumn("distance_group_id", (distanceGroupLower / 5).cast("int"))
    .withColumn("distance_group", concat(distanceGroupLower, lit("-"), distanceGroupLower + 5))
    .withColumn("trip_time", round(tripTime))
    .groupBy(col("distance_group_id"), col("distance_group"))
    .agg(
      count("distance_group").as("trips_qty"),
      format_number(avg("trip_distance"), 1).as("avg_distance"),
      round(avg("trip_time")).as("avg_trip_time"),
      format_number(sum("total_amount") / sum("trip_distance"), 2).as("avg_amount_per_mile"),
      min("total_amount").as("min_amount"),
      max("total_amount").as("max_amount")
    )
    .orderBy(col("distance_group_id"))
  }

  //--------------------------------------------------------------------

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder()
      .appName("WorkTaxiDataMartOnDataSet")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val taxiFactsDS = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
      .as[TaxiTrip]

    val taxiDataMartDS = processTaxiDataDS(taxiFactsDS)

    taxiDataMartDS.show()

    taxiDataMartDS.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/otus")
      .option("dbtable", "taxi_trips_by_distance_groups")
      .option("user", "docker")
      .option("password", "docker")
      .option("driver", "org.postgresql.Driver")
      .mode("overwrite")
      .save()
  }

}
