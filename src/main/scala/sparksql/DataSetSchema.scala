package sparksql

object DataSetSchema {

}

package sparksql

import org.apache.spark.sql.{Dataset, SparkSession}

case class DeviceIoTData
(
  battery_level: Long,
  c02_level: Long,
  cca2: String,
  cca3: String,
  cn: String,
  device_id: Long,
  device_name: String,
  humidity: Long,
  ip: String,
  latitude: Double,
  lcd: String,
  longitude: Double,
  scale:String,
  temp: Long,
  timestamp: Long,
)

object DataSetSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataSet Example").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val dataFile = "data/iot_devices.json"
    val ds: Dataset[DeviceIoTData] = spark.read.format("json").load(dataFile).as[DeviceIoTData]

    ds.show(5, false)

    spark.stop()
  }
}
