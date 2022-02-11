package scala.common

import scala.common.Utils.getDaysAgoAfter

/**
 * Created by Kunverse on 20220211.
 */
trait Scenario {

  var date: String = _
  var today: String = _
  var yesterday: String = _
  var xmlPath: String = _
  var specialAppName: String = _
  var argsMap: Map[String, String] = _
  var scenarioType: ScenarioType.Value

  def main(args: Array[String]): Unit = {
    println("Program BEGIN!")
    // input
    if (args.length != 2) {
      System.exit(1)
    }
    // date
    date = args(0)

    // xml
    xmlPath = args(1)

    // date calculation commonly used
    today = date
    yesterday = getDaysAgoAfter(date, -1)

    // Spark Sesssion Environment
    SparkSessionInit.init()

    // scenario
    val scenario = new SpecificScenario().analyze(xmlPath, scenarioType)
    specialAppName = scenario.specScenarioName
    argsMap = scenario.argsMap

    // This is the main function
    startMain()

    println("Program SUCCESS!")
  }

  protected def startMain()

}
