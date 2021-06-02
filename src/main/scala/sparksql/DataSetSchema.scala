package sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object DataSetSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataSet Example").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val dataFile = "data/iot_devices.json"
    val ds: Dataset[DeviceIoTData] = spark.read.format("json").load(dataFile).as[DeviceIoTData]

    val filterTempDS = ds.filter(row => {row.temp > 30 && row.humidity > 70})
    filterTempDS.show(5, false)

    // Using select will convert the DataSet into a DataFrame
    val dft: DataFrame = ds.select($"device_name", $"temp", $"humidity")
    dft.show(5)
    // Using where maintains the DataSet type
    val dft2: Dataset[DeviceIoTData] = ds.where("battery_level = 0")
    dft2.show(5)
    // Using filter maintains the DataSet type
    val dft3: Dataset[DeviceIoTData] = ds.filter(row => {row.temp > 25})
    dft3.show(5)
    // Using map still returns a DataSet
    val dft4: Dataset[(String, Long, Long)] = ds.map(row => (row.device_name, row.temp, row.humidity))
    dft4.show(5)

    val dfTemp: DataFrame = ds.filter(row => {row.temp > 25})
      .map(row => (row.device_name, row.temp, row.humidity))
      .toDF("DeviceName", "Temperature", "Humidity")

    dfTemp.show(5, false)

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
