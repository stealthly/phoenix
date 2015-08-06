package ly.stealth.phoenix

import java.io.{FileInputStream, File}
import java.util.Properties

object Config {
  def apply(file: File, overrides: Map[String, String]): Config = {
    val props = new Properties()
    props.load(new FileInputStream(file))

    overrides.foreach {
      case (k, v) =>
        props.setProperty(k, v)
    }

    new Config(props)
  }

  def apply(file: File): Config = apply(file, Map.empty)
}


class Config(props: Properties) {

  val MesosZk = props.getProperty("zk")
  val MesosMaster = props.getProperty("master")
  val RestApi = props.getProperty("api")
  val FrameworkName = props.getProperty("framework.name")

}
