package scala.senerio.usercluster

import scala.common.SparkSessionInit.spark
import scala.common.Utils.{getMaxPartiondateFromSpecHiveTable, printDf}
import scala.common.{Scenario, ScenarioType}

/**
  * Created by Kunverse on 20220211
  */
object UserClustering extends Scenario {

  override var scenarioType: ScenarioType.Value = ScenarioType.UserClustering
  var clusterNum: Int = 6
  var expireDays: Int = 30

  override protected def startMain(): Unit = {
    getArgs

    println(clusterNum)
    println(expireDays)

    val dfResult = getDataFromHive(today)

    printDf("dfResult", dfResult)
  }

  def getDataFromHive(partitiondate: String) = {
    val hiveTable = ""
    val maxPartitiondate = getMaxPartiondateFromSpecHiveTable(hiveTable, "partitiondate")
    println("maxPartitiondate: ", maxPartitiondate)
    val sqlLine =
      s"""
         |SELECT
         |    *
         |FROM
         |    ${hiveTable}
         |WHERE 1=1
         |    AND partitiondate='${maxPartitiondate}'
         |LIMIT 10
         |""".stripMargin
    println(sqlLine)

    val dfResult = spark.sql(sqlLine)

    dfResult
  }

  def getArgs: Unit = {
    expireDays = argsMap("expireDays").toInt
    clusterNum = argsMap("clusterNum").toInt
  }

}
