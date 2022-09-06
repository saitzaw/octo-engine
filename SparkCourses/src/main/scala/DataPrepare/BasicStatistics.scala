package DataPrepare

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{to_date, when}

object BasicStatistics {
  def csvFileReader(fileName: String): String = {
    val filePath = "resources/"
    val csvFile = filePath + fileName
    csvFile
  }

  def csvFileSaver(DFToSave: DataFrame, fileName: String): Unit = {
    val filePath = "product/"
    val csvFile = filePath + fileName
    DFToSave.write
      .option("header", "true")
      .mode("overwrite")
      .csv(csvFile)
  }

  def renameDF(DFToRename: DataFrame): DataFrame = {
    val newNames = Seq(
      "region",
      "country",
      "item",
      "sales_channel",
      "order_priority",
      "order_date",
      "order_id",
      "ship_date",
      "units_sold",
      "unit_price",
      "unit_cost",
      "total_revenue",
      "total_cost",
      "total_profit"
    )
    val RenamedDF = DFToRename.toDF(newNames: _*)
    RenamedDF
  }

  def convertDateFormat(DFToStandardDateFormat: DataFrame): DataFrame = {
    val dateColumns = Array("order_date", "ship_date")
    val completeConvertDF = dateColumns
      .foldLeft(DFToStandardDateFormat) { (newDF, colName) =>
        newDF.withColumn(
          colName,
          when(
            to_date(newDF(colName), "M/d/y").isNotNull,
            to_date(newDF(colName), "M/d/y")
          )
            .when(
              to_date(newDF(colName), "M/d/y").isNotNull,
              to_date(newDF(colName), "M/d/y")
            )
        )
      }
    completeConvertDF
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("FiltersApp")
      .master("local")
      .getOrCreate()

    val csvFileName = csvFileReader("2mSalesRecords.csv")

    val fileToAnalyst = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(csvFileName)

    val reformatColDF = renameDF(fileToAnalyst)
    val DFtoCheck = convertDateFormat(reformatColDF)

    /*
     * Select the total Sale in 2018-01-01 to 2018-12-31 in Panama
     * based on order date
     * */

    val panamaData = DFtoCheck.filter("country == 'Panama'")
    val panamaYear18Data =
      panamaData.filter("order_date BETWEEN '2018-01-01' AND '2018-12-31'")

    panamaYear18Data.show()
    panamaYear18Data.groupBy("item").sum("units_sold").show()
    panamaYear18Data.groupBy("item").max("units_sold").show()
    panamaYear18Data.groupBy("item").min("units_sold").show()
    panamaYear18Data.groupBy("item").avg("units_sold").show()

    csvFileSaver(panamaData, "panama_data.csv")

    spark.stop()
  }
}
