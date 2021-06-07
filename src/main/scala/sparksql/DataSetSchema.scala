package sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object DataSetSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataSet Example").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val dataFile = "data/iot_devices.json"
    val ds: Dataset[DeviceIoTData] = spark.read.format("json").load(dataFile).as[DeviceIoTData]

    val df: DataFrame = spark.read.format("json").load(dataFile)

    val filterTempDF: Dataset[Row] = df.where($"temp" > 30 && $"humidity" > 70)
//    val filterTempDF0: Dataset[Row] = df.where($"fjwqeroup" > 30 && $"humidity" > 70)

    val filterTempDS: Dataset[DeviceIoTData] = ds.filter(row => {row.temp > 30 && row.humidity > 70})
//    val filterTempDS0: Dataset[DeviceIoTData] = ds.filter(row => {row.owqiefjhpois > 30 && row.humidity > 70})

    filterTempDS.show(5, false)

    val dfTemp= ds.filter(row => {row.temp > 25})
      .map(row => (row.device_name, row.temp, row.humidity))
//      .toDF("DeviceName", "Temperature", "Humidity")

    println("Some unnamed DataSet")
    dfTemp.show(5)
//      .toDF("DeviceName", "Temperature", "Humidity")

    // Using select will convert the DataSet into a DataFrame
    val dft = ds.select($"device_name", $"temp", $"humidity")
    dft.show(5)
//    val dft0: DataFrame = ds.selectExpr("device_name AS Name")
    // Using where maintains the Dataset type
    val dft2: Dataset[DeviceIoTData] = ds.where("battery_level = 0")
    val dft20: Dataset[DeviceIoTData] = ds.where($"battery_level".equalTo(0))
    dft20.show(5)
    // Using filter maintains the DataSet type
    val dft3: Dataset[DeviceIoTData] = ds.filter(row => {row.temp > 25})
    dft3.show(5)
    // Using map still returns a DataSet
    val dft4: Dataset[(String, Long, Long)] = ds.map(row => (row.device_name, row.temp, row.humidity))
    dft4.show(5)

    // Detect failing devices with battery levels below a threshold.
    val batteryThresh = 5
    ds.filter(row => row.battery_level <= batteryThresh).show(5)

    //Identify offending countries with high levels of CO2 emissions.
    ds.groupBy("cn")
      .agg(sum("c02_level").alias("co2_emission"))
      .orderBy(desc("co2_emission"))
      .show(false)

    // Compute the min and max values for temperature, battery level, CO2, and humidity.
    ds.agg(min("humidity"), min("temp")).show()

    spark.stop()
  }
}
