import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileUtils {

  def readCsv(filePath: String, spark: SparkSession): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)
  }

  def readCsv(filePath: String, schema: StructType, spark: SparkSession): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .schema(schema)
      .load(filePath)
  }

}
