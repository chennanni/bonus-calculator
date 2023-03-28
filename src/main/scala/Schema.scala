import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StructField, StructType}

object Schema {
  val schemaSalary = new StructType()
    .add(StructField("Year", IntegerType, nullable = true))
    .add(StructField("Staff_Id", IntegerType, nullable = true))
    .add(StructField("Base_Salary", DoubleType, nullable = true))

  val schemaPerformance = new StructType()
    .add(StructField("Year", IntegerType, nullable = true))
    .add(StructField("Staff_Id", IntegerType, nullable = true))
    .add(StructField("Score", DoubleType, nullable = true))

  val schemaAttendance = new StructType()
    .add(StructField("Year", IntegerType, nullable = true))
    .add(StructField("Staff_Id", IntegerType, nullable = true))
    .add(StructField("Month", IntegerType, nullable = true))
    .add(StructField("Work_Days", IntegerType, nullable = true))
}
