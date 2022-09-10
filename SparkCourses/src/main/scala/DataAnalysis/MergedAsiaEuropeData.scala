package DataAnalysis

import org.apache.spark.sql.SparkSession

object MergedAsiaEuropeData {
  def csvFilePath(csvFileName: String): String = {
    val filePath = "product/"
    val csvFile = filePath + csvFileName
    csvFile
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("AsiaEuropeDataApp")
      .master("local")
      .getOrCreate()
    val csvFileName = csvFilePath("5mSaleRecordNormalized.csv")
    val csvFileToAnalyze = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(csvFileName)

    val AsianSaleRecordDF = csvFileToAnalyze.filter("region == 'Asia'")
    println(
      "total number of row count in Asian region: " + AsianSaleRecordDF.count()
    )

    val EuropeSaleRecordDF = csvFileToAnalyze.filter("region == 'Europe'")
    println(
      "total number of row count in Europe region: " + EuropeSaleRecordDF
        .count()
    )

    val mergedAsiaEuropeData = AsianSaleRecordDF.union(EuropeSaleRecordDF)

    mergedAsiaEuropeData.show()
    println("total row count: " + mergedAsiaEuropeData.count())

  }
}
