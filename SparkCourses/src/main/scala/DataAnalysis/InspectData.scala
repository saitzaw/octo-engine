package DataAnalysis

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, count, when}

object InspectData {
  def csvFilePath(csvFileName: String): String = {
    val filePath = "product/"
    val csvFile = filePath + csvFileName
    csvFile
  }

  def countCols(columns: Array[String]): Array[Column] = {
    columns.map(c => {
      count(
        when(
          col(c).isNull ||
            col(c) === "" ||
            col(c).contains("NULL") ||
            col(c).contains("null") ||
            col(c).contains("NA") ||
            col(c).contains("null") ||
            col(c).contains("NaN") ||
            col(c).contains("nan"),
          c
        )
      ).alias(c)
    })
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("InspectDataApp")
      .master("local")
      .getOrCreate()

    val csvFileName = csvFilePath("5mSaleRecordNormalized.csv")

    val csvFileToAnalyze = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(csvFileName)

    // Check the Null, NaN, empty string in Column
    csvFileToAnalyze.select(countCols(csvFileToAnalyze.columns): _*).show()

  }
}
