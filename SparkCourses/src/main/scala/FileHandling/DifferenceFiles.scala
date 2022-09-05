package FileHandling

import java.nio.file.Files
import java.nio.file.Paths
import org.apache.spark.sql.SparkSession
import java.io.FileNotFoundException

object DifferenceFiles {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("FileHandlingApp")
      .master("local")
      .getOrCreate()

    // Read from JSON
    try {
      val JSONFilePath = "resources/zipcodes.json"
      if (Files.exists(Paths.get(JSONFilePath))) {
        val zipcodeJsonDF = spark.read.json(JSONFilePath)

        //select required column
        val selectedZipCodeDF = zipcodeJsonDF
          .select(
            "City",
            "Country",
            "Lat",
            "Long",
            "Zipcode",
            "EstimatedPopulation"
          )

        // remove the Null value from dataframe
        val removeFromRecordIfPopulationIsNull =
          selectedZipCodeDF.na.drop(Seq("EstimatedPopulation"))

        // select records over 20000 population
        val selectLargePopulationDF = removeFromRecordIfPopulationIsNull
          .filter("EstimatedPopulation >= 20000")

        // show JSON dataframe
        selectLargePopulationDF.show(truncate = true)
      }
    } catch {
      case e: FileNotFoundException => println("Couldn't find file")
    }

    // Read from csv
    try {
      val csvFilePath = "resources/zipcodes.csv"
      if (Files.exists(Paths.get(csvFilePath))) {
        val zipcodeCSVDF = spark.read.option("header", true).csv(csvFilePath)

        //select required column
        val selectedCSVZipCodeDF = zipcodeCSVDF
          .select(
            "City",
            "Country",
            "Lat",
            "Long",
            "Zipcode",
            "EstimatedPopulation"
          )

        // remove the Null value from dataframe
        val removeFromRecordIfPopulationIsNullCSV =
          selectedCSVZipCodeDF.na.drop(Seq("EstimatedPopulation"))

        // select records over 10000 population
        val selectCSVLargePopulationDF = removeFromRecordIfPopulationIsNullCSV
          .filter("EstimatedPopulation >= 10000")

        // show JSON dataframe
        selectCSVLargePopulationDF.show(truncate = true)
      }
    } catch {
      case e: FileNotFoundException => println("Couldn't find file")
    }
    spark.stop()
  }
}
