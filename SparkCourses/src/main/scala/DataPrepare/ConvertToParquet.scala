package DataPrepare

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.functions.{to_date, when}

object ConvertToParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("csvToParquetApp")
      .master("local")
      .getOrCreate()

    val filePath = "resources/"
    val recordedFileName = "5mSalesRecords.csv"

    /*
     * Schema definitions for 5M records
     * Let Date column be String type
     */
    val schemaToChange = StructType(
      Array(
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
    )

    // read csv file
    val twoMCSVFile =
      spark.read
        .option("header", value = true)
        .option("inferSchema", value = true)
        .option("dateFormat", "yyyyMMdd")
        .schema(schemaToChange)
        .csv(filePath + recordedFileName)

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

    // rename the dataframe column name
    val RenamedDF = twoMCSVFile.toDF(newNames: _*)

    val dateColumns = Array("order_date", "ship_date")

    val completeDF = dateColumns
      .foldLeft(RenamedDF) { (newDF, colName) =>
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

    completeDF.show(truncate = false)

    completeDF.write
      .option("compression", "none")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/tmp/2mSalesRecord")

  }
}
