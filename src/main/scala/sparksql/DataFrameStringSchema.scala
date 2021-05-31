package sparksql

import org.apache.spark.sql.SparkSession

object DataFrameStringSchema {
  def main(args: Array[String]): Unit = {
    // Additional dependencies needed for "enableHiveSupport"
    val spark = SparkSession.builder().enableHiveSupport().master("local[2]").appName("DataFrame String Schema").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val schema =
      """
        |Id STRING, First STRING, Last STRING, Url STRING, Published STRING, Hits INT, Campaigns ARRAY<STRING>
        |""".stripMargin

    val dataFile = "data/blogs.json"
    val df = spark.read.schema(schema).format("json").load(dataFile)

    df.show()

    df.printSchema()
    println(df.schema)

    spark.stop()
  }
}
