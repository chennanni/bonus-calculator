import FileUtils.readCsv
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, expr, round, split}

object App {

  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("==========Application Start==========")

    val spark = SparkSession.builder()
      .appName("Bonus Calculator")
      .master("local[*]")
      .getOrCreate()

    println("==========Load tables==========")

    println("Loading table: Performance Metrics")
    val dfPerformanceMetrics: DataFrame = readCsv("src/main/resources/static_tables/Performance_Metrics.csv", spark)
    dfPerformanceMetrics.show()

    println("Loading table: Attendance Metrics")
    val dfAttendanceMetrics: DataFrame = readCsv("src/main/resources/static_tables/Attendance_Metrics.csv", spark)
    dfAttendanceMetrics.show()

    println("Loading table: Profit")
    val dfProfit: DataFrame = readCsv("src/main/resources/dynamic_tables/Profit.csv", spark)
    dfProfit.show()

    println("Loading table: Overtime")
    val dfOvertime: DataFrame = readCsv("src/main/resources/dynamic_tables/Overtime.csv", spark)
    dfOvertime.show()

    println("Loading table: Salary")
    val dfSalary: DataFrame = readCsv("src/main/resources/dynamic_tables/Salary.csv", Schema.schemaSalary, spark)
    dfSalary.show()

    println("Loading table: Performance")
    val dfPerformance: DataFrame = readCsv("src/main/resources/dynamic_tables/Performance.csv", Schema.schemaPerformance, spark)
    dfPerformance.show()

    println("Loading table: Attendance")
    val dfAttendance: DataFrame = readCsv("src/main/resources/dynamic_tables/Attendance.csv", Schema.schemaAttendance, spark)
    dfAttendance.show()

    println("==========Calculation==========")

    // Overtime Pay Calculation

    // 1. salary join with overtime & company profit
    val dfOvertimeJoins = dfSalary
      .join(dfOvertime, Seq("Year", "Staff_Id"), "right")
      .join(dfProfit, Seq("Year"), "left")

    // 2. do the calculation
    val dfOvertimeBonusDetails = dfOvertimeJoins.withColumn("Overtime_Pay",
      round(col("Base_Salary") / 12 / 30
          * col("Hour") / 8
          * col("Multiplier")
        , 2
      )).select(col("Year")
        , col("Staff_Id")
        , col("Date").alias("Overtime_Date")
        , col("Hour").alias("Overtime_Hour")
        , col("Overtime_Pay").alias("Overtime_Bonus"))

    // 3. sum
    val dfOvertimeBonus = dfSalary
      .join(dfOvertimeBonusDetails, Seq("Year", "Staff_Id"),"left")
      .select("Year", "Staff_Id", "Overtime_Bonus")
      .groupBy("Year", "Staff_Id")
      .agg(
        functions.sum("Overtime_Bonus").alias("Overtime_Bonus")
      )
      .orderBy("Year", "Staff_Id")
      .na.fill(Map(
      "Overtime_Bonus" -> 0
    ))

    println("Bonus_Overtime")
    dfOvertimeBonus.show()
    println("Bonus_Overtime_Details")
    dfOvertimeBonusDetails.show()

    // Performance Calculation

    // 1. calculate performance multiplier
    val dfPerformanceMetricsWithBound = dfPerformanceMetrics
      .withColumn("LowerBound", expr("substring(Score_Range, 2, instr(Score_Range, ',') - 2)").cast("double"))
      .withColumn("UpperBound", expr("substring(Score_Range, instr(Score_Range, ',') + 1, instr(Score_Range, ')') - instr(Score_Range, ',') - 1)").cast("double")-0.01)
      .drop("Score_Range")

    val dfPerDetails = dfPerformance.join(dfPerformanceMetricsWithBound, col("Score")
      .between(col("LowerBound"), col("UpperBound")), "left")
      .select(col("Year"), col("Staff_Id"), col("Score"), col("Multiplier").alias("Performance_Multiplier"))

    // 2. calculate attendance multiplier
    val dfAttendanceMetricsWithBound = dfAttendanceMetrics
      .withColumn("LowerBound", expr("substring(Days_Range, 2, instr(Days_Range, ',') - 2)").cast("double"))
      .withColumn("UpperBound", expr("substring(Days_Range, instr(Days_Range, ',') + 1, instr(Days_Range, ')') - instr(Days_Range, ',') - 1)").cast("double")-0.01)
      .drop("Days_Range")

    val dfAttDetails = dfAttendance.join(dfAttendanceMetricsWithBound, col("Work_Days")
      .between(col("LowerBound"), col("UpperBound")), "left")
      .select(col("Year"), col("Staff_Id"), col("Month"), col("Work_Days"), col("Multiplier").alias("Attendance_Multiplier"))

    val dfAttDetailsSum = dfAttDetails
      .groupBy("Year", "Staff_Id")
      .agg(
        functions.sum("Attendance_Multiplier") alias("Attendance_Multiplier")
      )
      .orderBy("Year", "Staff_Id")

    // 3. sum
    val dfPerformanceBonus = dfSalary
      .join(dfAttDetailsSum, Seq("Year", "Staff_Id"), "left")
      .join(dfPerDetails, Seq("Year", "Staff_Id"), "left")
      .join(dfProfit, Seq("Year"), "left")
      .withColumn("Performance_Bonus",
          round(col("Base_Salary") / 12
            * col("Performance_Multiplier")
            * col("Attendance_Multiplier")
            * col("Multiplier")
            , 2)
      ).select(col("Year")
      , col("Staff_Id")
      , col("Performance_Bonus")
    )

    println("Bonus_Performance")
    dfPerformanceBonus.show()

    println("Bonus_Performance_Details_A")
    dfPerDetails.show()

    println("Bonus_Performance_Details_B")
    dfAttDetails.show()

    // Overall Bonus Calculation

    val dfBonus = dfOvertimeBonus
      .join(dfPerformanceBonus, Seq("Year", "Staff_Id"),"inner")
      .withColumn("Overall_Bonus",
        round(col("Overtime_Bonus") + col("Performance_Bonus"), 2))
      .join(dfSalary, Seq("Year", "Staff_Id"),"left")

    println("Bonus_Overall")
    dfBonus.show()

    logger.info("==========Application End==========")
  }

}