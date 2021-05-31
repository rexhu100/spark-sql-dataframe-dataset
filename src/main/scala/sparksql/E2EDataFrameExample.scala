package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object E2EDataFrameExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("End-to-End DF Example").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val schema = StructType(Array(
    StructField("CallNumber", IntegerType, true),
    StructField("UnitID", StringType, true),
    StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true),
    StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true),
    StructField("CallFinalDisposition", StringType, true),
    StructField("AvailableDtTm", StringType, true),
    StructField("Address", StringType, true),
    StructField("City", StringType, true),
    StructField("Zipcode", IntegerType, true),
    StructField("Battalion", StringType, true),
    StructField("StationArea", StringType, true),
    StructField("Box", StringType, true),
    StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true),
    StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true),
    StructField("CallTypeGroup", StringType, true),
    StructField("NumAlarms", IntegerType, true),
    StructField("UnitType", StringType, true),
    StructField("UnitSequenceInCallDispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true),
    StructField("SupervisorDistrict", StringType, true),
    StructField("Neighborhood", StringType, true),
    StructField("Location", StringType, true),
    StructField("RowID", StringType, true),
    StructField("Delay", FloatType, true)
    ))

    val dataFile = "data/sf-fire-calls.csv"
    var df = spark.read.format("csv").schema(schema).option("header", true).load(dataFile)
    df = df.withColumn("CallDate", to_date($"CallDate", "MM/dd/yyyy"))

    df.show(5, false)
    df.printSchema()

    // What were all the different types of fire calls in 2018?
    println("All call types in 2018 (ordered by frequency in descending order)")
    df.where($"CallDate" >= "2018-01-01" && $"CallDate" <= "2018-12-31")
      .groupBy("CallType")
      .agg(count("CallNumber").alias("count"))
      .orderBy(desc("count"))
      .show()

    // What months within the year 2018 saw the highest number of fire calls?
    println("Month with the highest number of calls")
    df.where("CallDate BETWEEN '2018-01-01' AND '2018-12-31'")
      .withColumn("CallMonth", month($"CallDate"))
      .groupBy("CallMonth")
      .agg(Map("CallNumber" -> "count"))
      .withColumnRenamed("count(CallNumber)", "count")
      .orderBy(desc("count"))
      .show(1)

    // Which neighborhood in San Francisco generated the most fire calls in 2018?
    println("Neighborhood with the highest number of calls")
    df.where("CallDate BETWEEN '2018-01-01' AND '2018-12-31'")
      .groupBy("Neighborhood")
      .agg(count("CallNumber").alias("count"))
      .orderBy(desc("count"))
      .show(1)

    // Which neighborhoods had the worst response times to fire calls in 2018?
    println("Neighborhood with the worst average response delay")
    df.where("CallDate BETWEEN '2018-01-01' AND '2018-12-31'")
      .groupBy("Neighborhood")
      .agg(avg("Delay").alias("averageDelay"))
      .orderBy(desc("averageDelay"))
      .show(1)

    // Which week in the year in 2018 had the most fire calls?
    println("Week of the year with the highest number of calls")
    df.where("CallDate BETWEEN '2018-01-01' AND '2018-12-31'")
      .withColumn("dayOfTheYear", date_format($"CallDate", "D"))
      .withColumn("weekOfTheYear", $"dayOfTheYear" / 7)
      .withColumn("weekOfTheYear", $"weekOfTheYear".cast(IntegerType))
      .groupBy("weekOfTheYear")
      .agg(count("CallNumber").alias("count"))
      .orderBy(desc("count"))
      .show(1)

    // Write the file into a Parquet file
    val savePath = "output/sf-fire-calls.parquet"
    df.write.format("parquet").mode("overwrite").save(savePath)

    spark.stop()
  }
}
