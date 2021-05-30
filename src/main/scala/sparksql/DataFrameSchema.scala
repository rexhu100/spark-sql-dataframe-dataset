package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataFrameSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataFrame Schema").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val schema = StructType(Array(
      StructField("Id", IntegerType, false), StructField("First", StringType, false),
      StructField("Last", StringType, false), StructField("Url", StringType, false),
      StructField("Published", StringType, false), StructField("Hits", LongType, false),
      StructField("Campaigns", ArrayType(StringType), false)
    ))

    val dataFile = "data/blogs.json"

    val df = spark.read.schema(schema).format("json").load(dataFile)

    df.show()
    df.printSchema()
    println(df.schema)

    spark.stop()
  }
}
