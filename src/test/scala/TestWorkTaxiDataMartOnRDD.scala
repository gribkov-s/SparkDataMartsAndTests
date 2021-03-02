
import ru.otus.hadoop.homework2.sgribkov.{ReadData, TaxiTrip}
import ru.otus.hadoop.homework2.sgribkov.WorkTaxiDataMartOnRDD.processTaxiDataRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class TestWorkTaxiDataMartOnRDD extends AnyFlatSpec with ReadData {

  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("TestWorkTaxiDataMartOnRDD")
    .getOrCreate()

  import spark.implicits._

  it should "upload and process data" in {

    val taxiFactsRDD:  RDD[TaxiTrip] = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
      .as[TaxiTrip]
      .rdd

    val taxiDataMartRDDSample = processTaxiDataRDD(taxiFactsRDD)
      .collect()
      .head

    assert(taxiDataMartRDDSample._1 == 19)
    assert(taxiDataMartRDDSample._2 == 22121)
  }

}
