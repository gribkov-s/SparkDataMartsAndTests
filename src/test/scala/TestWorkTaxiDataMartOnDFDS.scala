
import ru.otus.hadoop.homework2.sgribkov.WorkTaxiDataMartOnDataFrame.processTaxiDataDF
import ru.otus.hadoop.homework2.sgribkov.WorkTaxiDataMartOnDataSet.processTaxiDataDS
import ru.otus.hadoop.homework2.sgribkov.{ReadData, TaxiTrip}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{Row, SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.scalatest.funsuite.AnyFunSuite


class TestWorkTaxiDataMartOnDFDS extends AnyFunSuite with SharedSparkSession with ReadData {

  import testImplicits._

  test("taxiDataMartDS") {

    val taxiFactsDS = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
      .as[TaxiTrip]

    val taxiDataMartDS = processTaxiDataDS(taxiFactsDS)

    checkAnswer(
      taxiDataMartDS,
      Row(0, "0-5", 287220, 1.6, 14.0, 7.36, 0.3, 980.36)  ::
      Row(1, "5-10", 25554, 7.1, 29.0, 4.44, 0.3, 150.3)  ::
      Row(2, "10-15", 9292, 11.7, 41.0, 4.17, 0.3, 153.56)  ::
      Row(3, "15-20", 6200, 17.5, 51.0, 3.83, 0.3, 295.55)  ::
      Row(4, "20-25", 1308, 21.4, 53.0, 3.38, 3.3, 269.06)  ::
      Row(5, "25-30", 235, 27.3, 64.0, 3.34, 52.8, 240.56)  ::
      Row(6, "30-35", 42, 31.9, 59.0, 3.73, 52.8, 250.8)  ::
      Row(7, "35-40", 17, 37.1, 73.0, 4.49, 57.3, 279.82)  ::
      Row(8, "40-45", 14, 41.9, 73.0, 4.10, 52.8, 297.9)  ::
      Row(9, "45-50", 5, 47.4, 93.0, 3.69, 136.8, 208.06)  ::
      Row(10, "50-55", 3, 52.2, 87.0, 4.22, 172.26, 294.56)  ::
      Row(11, "55-60", 1, 55.4, 66.0, 5.77, 319.88, 319.88)  ::
      Row(13, "65-70", 1, 66.0, 78.0, 2.25, 148.7, 148.7) :: Nil
    )
  }

  test("taxiDataMartDF") {

    val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZonesDF = readCSV("src/main/resources/data/taxi_zones.csv")

    val taxiDataMartDF = processTaxiDataDF(taxiFactsDF, taxiZonesDF)

    checkAnswer(
      taxiDataMartDF,
      Row("Manhattan", 296527)  ::
      Row("Queens", 13819)  ::
      Row("Brooklyn", 12672)  ::
      Row("Unknown", 6714)  ::
      Row("Bronx", 1589)  ::
      Row("EWR", 508)  ::
      Row("Staten Island", 64) :: Nil
    )
  }

}
