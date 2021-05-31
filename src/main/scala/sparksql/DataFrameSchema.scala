package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataFrameSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataFrame Schema").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val schema = StructType(Array(
      StructField("Id", IntegerType, false), StructField("First", StringType, false),
      StructField("Last", StringType, false), StructField("Url", StringType, false),
      StructField("Published", StringType, false), StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)
    ))

    val dataFile = "data/blogs.json"

    var df = spark.read.schema(schema).format("json").load(dataFile)

    df.show()
    df.printSchema()
    println(df.schema)

    // Parse the string field into date
    df = df.withColumn("Published", to_date(col("Published"), "M/d/yyyy"))
    df.show()
    df.printSchema()
    df.where(col("Published") < "2018-01-01").show()

    val newRows: Seq[Row] = Seq(
      Row(7, "John", "Doe", "https://foo.bar", "2020-01-01", 1000, Array("LinkedIn", "Instagram")),
      Row(8, "Jane", "Doe", "https://bar.baz", "2021-01-01", 9000, Array("Ins", "小红书")),
    )
    // createDataFrame method is kind of a stickler. Seq[Row] type cannot be directly used. I have to convert it to RDD[Row]
    val dfNewRow: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(newRows), schema)

    df = df.union(dfNewRow)
    df.show()

    spark.stop()
  }
}

