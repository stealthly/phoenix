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

  val MesosZk = props.getProperty("mesos.zk")
  val MesosMaster = props.getProperty("mesos.master")
  val FrameworkName = props.getProperty("mesos.framework.name")

  val RestApi = props.getProperty("phoenix.api")

  // MUST SET FOR SECOR

  val SecorKafkaTopicFilter = props.getProperty("secor.kafka.topic_filter")
  val AwsAccessKey = props.getProperty("aws.access.key")
  val AwsSecretKey = props.getProperty("aws.secret.key")

  val SecorS3Bucket = props.getProperty("secor.s3.bucket")
  val KafkaSeedBrokerHost = props.getProperty("kafka.seed.broker.host")
  val ZookeeperQuorum = props.getProperty("zookeeper.quorum")
}
