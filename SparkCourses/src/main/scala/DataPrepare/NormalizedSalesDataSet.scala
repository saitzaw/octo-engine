package DataPrepare

import org.apache.spark.sql.functions.{to_date, when}
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFrame, SparkSession}

object NormalizedSalesDataSet {

  def csvFilePath(csvFileName: String): String = {
    val filePath = "resources/"
    val csvFile = filePath + csvFileName
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

  def schemaToChange(): Array[StructField] = {
    val schemaToChange = Array(
      StructField("Region", StringType, nullable = true),
      StructField("Country", StringType, nullable = true),
      StructField("Item Type", StringType, nullable = true),
      StructField("Sales Channel", StringType, nullable = true),
      StructField("Order Priority", StringType, nullable = true),
      StructField("Order Date", StringType, nullable = true),
      StructField("Order ID", IntegerType, nullable = true),
      StructField("Ship Date", StringType, nullable = true),
      StructField("Units Sold", IntegerType, nullable = true),
      StructField("Unit Price", DoubleType, nullable = true),
      StructField("Unit Cost", DoubleType, nullable = true),
      StructField("Total Revenue", DoubleType, nullable = true),
      StructField("Total Cost", DoubleType, nullable = true),
      StructField("Total Profit", DoubleType, nullable = true)
    )
    schemaToChange
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
      .appName("DataCollectionApp")
      .master("local")
      .getOrCreate()

    val csvFileName = csvFilePath("5mSalesRecords.csv")
    val definedSchema = StructType(schemaToChange())
    val salesRecords = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .schema(definedSchema)
      .csv(csvFileName)

    val renameSaleRecord = renameDF(salesRecords)
    val normalizedDateForSaleRecord = convertDateFormat(renameSaleRecord)
    csvFileSaver(normalizedDateForSaleRecord, "5mSaleRecordNormalized.csv")
  }
}
