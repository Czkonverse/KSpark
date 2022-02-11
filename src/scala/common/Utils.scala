package scala.common

import scala.common.SparkSessionInit.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat
import java.util.Calendar

object Utils {

  def printDf(df_name: String, df: DataFrame, nums: Int = 40) = {

    println("_____________________\n" * 2)
    println(df_name)
    println("_____________________\n")
    df.show(nums, truncate = false)
    println("_____________________\n")
    df.printSchema()
    println("_____________________\n" * 2)
  }

  def printCountDf(df_name: String, df: DataFrame, nums: Int = 40) = {

    println("_____________________\n" * 2)
    println(df_name)
    println("_____________________\n")
    df.show(nums, truncate = false)
    println("_____________________\n")
    println(df.count())
    println("_____________________\n")
    df.printSchema()
    println("_____________________\n" * 2)
  }

  /**
   * @param days : 3 means 3 days later; -3 means 3 days ago.
   */
  def getDaysAgoAfter(dateTimeStringEightFormat: String, days: Int): String = {

    val sdf = new SimpleDateFormat("yyyyMMdd")
    val dt = sdf.parse(dateTimeStringEightFormat)
    val rightNow = Calendar.getInstance()
    rightNow.setTime(dt)
    rightNow.add(Calendar.DATE, days)
    val dt1 = rightNow.getTime
    val reStr = sdf.format(dt1)

    reStr
  }

  /**
   * Get the max partitiondate of a hive table which has a partition named "partitiondate".
   */
  def getMaxPartiondateFromSpecHiveTable(hiveTable: String, partitiondateName: String) = {

    def udfParsePartition = udf(parsePartition _)

    def parsePartition(partitionInfo: String): Map[String, String] = {

      partitionInfo
        .split("/")
        .map(elem => {
          val array = elem.split("=")
          val columnName = array(0)
          val value = array(1)
          Map(columnName -> value)
        }).reduce((x, y) => x.++(y))
    }

    val dfPartitionInfos = spark.sql(s"show partitions ${hiveTable}")
      .withColumn("temp", udfParsePartition(col("partition")))
      .withColumn(partitiondateName, col("temp").getItem(partitiondateName))

    val maxData = dfPartitionInfos.agg(max(partitiondateName))
    val maxpartitiondate = maxData.head.get(0).toString

    maxpartitiondate
  }
}
