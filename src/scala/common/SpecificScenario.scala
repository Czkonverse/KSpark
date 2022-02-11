package scala.common

import scala.xml.{NodeSeq, XML}

/**
  * Created by Kunverse on 20220211.
  */
class SpecificScenario {

  var specScenarioName: String = _
  var argsMap: Map[String, String] = Map()

  def analyze(xmlPath: String, scenarioType: ScenarioType.Value): this.type = {
    val xmlFile = XML.load(xmlPath)
    val xmlNodes = xmlFile \\ "SpecificScenario"
    val xmlNodesFilter = xmlNodes.filter(xmlNode => {
      val name = (xmlNode \ "Name").text.toLowerCase()
      name == scenarioType.toString.toLowerCase()
    })
    if (xmlNodesFilter.length == 0) {
      System.exit(1)
    }

    val xmlNode = xmlNodesFilter.head

    specScenarioName = (xmlNode \ "SpecScenario").text.toLowerCase()

    val argsNodes: NodeSeq = xmlNode \\ "Argument"
    argsNodes.foreach(f = argsNode => {
      val name = (argsNode \ "Name").text
      val value = (argsNode \ "Value").text
      argsMap += (name -> value)
    })

    this
  }

}
