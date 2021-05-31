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

    // Download the data from https://github.com/databricks/LearningSparkV2
    val dataFile = "data/blogs.json"
    val df = spark.read.schema(schema).format("json").load(dataFile)

    df.show()

    df.printSchema()
    println(df.schema)

    df.createOrReplaceTempView("blog_data")

    val dfTemp = spark.sql("SELECT TO_DATE(CAST(UNIX_TIMESTAMP(Published, 'M/d/yyyy') AS TIMESTAMP)) as PubDate FROM blog_data")
    dfTemp.show()

    spark.stop()
  }
}
