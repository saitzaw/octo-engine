package DataPrepare

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{to_date, when}

object DataFilterBasedOnDate {
  def csvFileReader(fileName: String): String = {
    val filePath = "resources/"
    val csvFile = filePath + fileName
    csvFile
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
     * Select Panama
     * Check total unit sold [online & offline]
     * Check total revenue and total cost for these
     * */

    // online sale data
    val onlineSales =
      DFtoCheck.filter("country == 'Panama' AND sales_channel == 'Online'")
    onlineSales.show(truncate = false)
    println("Total number of Online Sales is: " + onlineSales.count())

    val offlineSales =
      DFtoCheck.filter(("country == 'Panama' AND sales_channel == 'Offline'"))
    offlineSales.show(truncate = false)
    println("Total number of Offline Sales is: " + offlineSales.count())

    val totalOnlineUnitSold = onlineSales.groupBy("item").sum("units_sold")
    totalOnlineUnitSold.show()

    val totalOfflineUnitSold = offlineSales.groupBy("item").sum("units_sold")
    totalOfflineUnitSold.show()

    /*
     * Select the total Sale in 2018-01-01 to 2018-12-31 in Panama
     * based on order date
     * */

    val panamaData = DFtoCheck.filter("country == 'Panama'")

    val panamaYear18Data =
      panamaData.filter("order_date between '2018-01-01' and '2018-12-31'")
    panamaYear18Data.show()
    panamaYear18Data.groupBy("item").sum("units_sold").show()
    panamaYear18Data.groupBy("item").max("units_sold").show()
    panamaYear18Data.groupBy("item").min("units_sold").show()
    panamaYear18Data.groupBy("item").avg("units_sold").show()

    spark.stop()
  }
}
