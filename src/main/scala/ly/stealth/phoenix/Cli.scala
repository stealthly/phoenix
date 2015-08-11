package ly.stealth.phoenix

import java.io.{DataOutputStream, File, IOException, PrintStream}
import java.net.{HttpURLConnection, URL, URLEncoder}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scopt.{Read, OptionParser}

import scala.io.Source

object Cli {
  implicit val objectMapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private val out: PrintStream = System.out

  def main(args: Array[String]) {
    try {
      exec(args)
    } catch {
      case e: Throwable =>
        System.err.println("Error: " + e.getMessage)
        sys.exit(1)
    }
  }

  def exec(args: Array[String]) {
    if (args.length == 0) {
      handleHelp()
      printLine()
      throw CliError("No command supplied")
    }

    val command = args.head
    val commandArgs = args.tail

    command match {
      case "help" => if (commandArgs.isEmpty) handleHelp() else handleHelp(commandArgs.head)
      case "scheduler" => handleScheduler(commandArgs)
      case "add" => handleAdd(commandArgs)
      case "delete" => handleDelete(commandArgs)
      case "status" => handleStatus(commandArgs)
      case _ => throw CliError(s"Unknown command: $command\n")
    }
  }

  def handleHelp(command: String = "") {
    command match {
      case "" =>
        printLine("Usage: <command>\n")
        printGenericHelp()
      case "scheduler" => Parsers.scheduler.showUsage
      case "add" => Parsers.add.showUsage
      case "delete" => Parsers.delete.showUsage
      case "status" => Parsers.status.showUsage
      case _ =>
        printLine(s"Unknown command: $command\n")
        printGenericHelp()
    }
  }

  def handleScheduler(args: Array[String]) {
    Parsers.scheduler.parse(args, Map()) match {
      case Some(config) =>
        val configFile = new File(config("config"))

        val apiOverrideOpt = config.get("api").orElse(Option(System.getenv("EM_API")))
        val awsAccessKeyOverrideOpt = config.get("aws-access-key").orElse(Option(System.getenv("AWS_ACCESS_KEY_ID")))
        val awsSecretKeyOverrideOpt = config.get("aws-secret-key").orElse(Option(System.getenv("AWS_SECRET_ACCESS_KEY")))

        val overrides = Map("phoenix.api" -> apiOverrideOpt, "aws.access.key" -> awsAccessKeyOverrideOpt,
          "aws.secret.key" -> awsSecretKeyOverrideOpt).collect { case (k, Some(v)) => (k, v)}

        val schedulerConfig = Config(configFile, overrides)
        val scheduler = new Scheduler(schedulerConfig)

        scheduler.start()
      case None => throw CliError("Invalid arguments")
    }
  }

  def handleAdd(args: Array[String]) {
    Parsers.add.parse(args, AddConfig(-1)) match {
      case Some(config) =>
        val httpServer = resolveApi(config.api)

        val addServerRequest = AddServerRequest(config.id, config.cpus, config.mem, config.overrides)
        val response = sendAddRequest(httpServer, addServerRequest)
        printLine(response.message)
        printLine()
        response.value.foreach(printCluster)
      case None => throw CliError("Invalid arguments")
    }
  }

  def handleDelete(args: Array[String]) {
    Parsers.delete.parse(args, Map()) match {
      case Some(config) =>
        val httpServer = resolveApi(config.get("api"))

        val response = sendDeleteRequest(httpServer, DeleteServerRequest(config("id").toInt))
        printLine(response.message)
        printLine()
      case None => throw CliError("Invalid arguments")
    }
  }

  def handleStatus(args: Array[String]) {
    Parsers.status.parse(args, Map()) match {
      case Some(config) =>
        val httpServer = resolveApi(config.get("api"))

        val cluster = sendStatusRequest(httpServer)
        printCluster(cluster.value.get)
      case None => throw CliError("Invalid arguments")
    }
  }

  private def resolveApi(apiOption: Option[String]): String = {
    apiOption
      .orElse(Option(System.getenv("EM_API")))
      .getOrElse(throw CliError("Undefined API url. Please provide either a CLI --api option or EM_API env."))
  }

  private def sendAddRequest(httpServer: String, addServerRequest: AddServerRequest): ApiResponse = {
    val url: String = httpServer + (if (httpServer.endsWith("/")) "" else "/") + "api/add"

    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("POST")
    connection.setDoOutput(true)
    val wr = new DataOutputStream(connection.getOutputStream)
    objectMapper.writeValue(wr, addServerRequest)
    wr.flush()
    wr.close()

    var response: String = null
    try {
      try {
        response = Source.fromInputStream(connection.getInputStream).getLines().mkString
      }
      catch {
        case e: IOException =>
          if (connection.getResponseCode != 200) throw new IOException(connection.getResponseCode + " - " + connection.getResponseMessage)
          else throw e
      }
    } finally {
      connection.disconnect()
    }

    objectMapper.readValue(response, classOf[ApiResponse])
  }

  private def sendDeleteRequest(httpServer: String, deleteServerRequest: DeleteServerRequest): ApiResponse = {
    val url: String = httpServer + (if (httpServer.endsWith("/")) "" else "/") + "api/delete"

    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("POST")
    connection.setDoOutput(true)
    val wr = new DataOutputStream(connection.getOutputStream)
    objectMapper.writeValue(wr, deleteServerRequest)
    wr.flush()
    wr.close()

    var response: String = null
    try {
      try {
        response = Source.fromInputStream(connection.getInputStream).getLines().mkString
      }
      catch {
        case e: IOException =>
          if (connection.getResponseCode != 200) throw new IOException(connection.getResponseCode + " - " + connection.getResponseMessage)
          else throw e
      }
    } finally {
      connection.disconnect()
    }

    println(response)
    objectMapper.readValue(response, classOf[ApiResponse])
  }

  private def sendStatusRequest(httpServer: String): ApiResponse = {
    val url: String = httpServer + (if (httpServer.endsWith("/")) "" else "/") + "api/status"

    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    var response: String = null
    try {
      try {
        response = Source.fromInputStream(connection.getInputStream).getLines().mkString
      }
      catch {
        case e: IOException =>
          if (connection.getResponseCode != 200) throw new IOException(connection.getResponseCode + " - " + connection.getResponseMessage)
          else throw e
      }
    } finally {
      connection.disconnect()
    }

    println(response)
    objectMapper.readValue(response, classOf[ApiResponse])
  }

  private def printLine(s: AnyRef = "", indent: Int = 0) = out.println("  " * indent + s)

  private def printGenericHelp() {
    printLine("Commands:")
    printLine("help       - print this message.", 1)
    printLine("help [cmd] - print command-specific help.", 1)
    printLine("scheduler  - start scheduler.", 1)
    printLine("status     - print cluster status.", 1)
    printLine("add        - add servers to cluster.", 1)
    printLine("delete     - delete servers in cluster.", 1)
  }

  private def printCluster(cluster: Cluster) {
    printLine("cluster:")
    cluster.servers.foreach(s => printServer(s))
  }

  private def printServer(server: Server, indent: Int = 0) {
    printLine("server:", indent)
    printLine(s"id: ${server.id}", indent + 1)
    printLine(s"state: ${server.state}", indent + 1)
    printTaskConfig(server.taskData, server.requestTemplate, indent + 1)
    printLine()
  }

  private def printTaskConfig(taskData:Option[TaskData], config: RequestTemplate, indent: Int) {
    printLine("server request template:", indent)
    val cpus = taskData.map(_.cpus).orElse(config.cpus)
    val mem = taskData.map(_.mem).orElse(config.mem)
    printLine(s"cpu: $cpus", indent)
    printLine(s"mem: $mem", indent)
    printLine(s"config overrides: ${config.configOverrides}", indent)
  }

  case class AddConfig(id: Int,
                       cpus: Option[Double] = None,
                       mem: Option[Double] = None,
                       api: Option[String] = None,
                       overrides: Map[String, String] = Map.empty)

  private object Parsers {
    val scheduler = new CliOptionParser("scheduler") {
      opt[String]('c', "config").required().text("Path to config file. Required.").action { (value, config) =>
        config.updated("config", value)
      }

      opt[String]('a', "api").optional().text("Binding host:port for http/artifact server. Optional if defined in config or EM_API env is set.").action { (value, config) =>
        config.updated("api", value)
      }

      opt[String]("aws-access-key").optional().text("Aws access key. Optional if defined in config file or env AWS_ACCESS_KEY_ID is set.").action { (value, config) =>
        config.updated("aws-access-key", value)
      }

      opt[String]("aws-secret-key").optional().text("Aws secret key. Optional if defined in config file or env AWS_SECRET_ACCESS_KEY is set.").action { (value, config) =>
        config.updated("aws-secret-key", value)
      }
    }

    def reads[A](f: String => A): Read[A] = new Read[A] {
      val arity = 1
      val reads = f
    }
    implicit val intOptRead: Read[Option[Int]]             = reads {s => if (s == null) None else Some(s.toInt) }
    implicit val doubleOptRead: Read[Option[Double]]             = reads {s => if (s == null) None else Some(s.toDouble) }
    implicit val stringOptRead: Read[Option[String]]             = reads {s => Option(s) }

    val add = new OptionParser[AddConfig]("add") {
      override def showUsage {
        Cli.out.println(usage)
        printLine()
      }

      opt[Int]('i', "id").required().text(s"Server id. Required.").action { (value, config) =>
        config.copy(id = value)
      }

      opt[Option[Double]]('c', "cpu").optional().text(s"CPUs for server. Optional.").action { (value, config) =>
        config.copy(cpus = value)
      }

      opt[Option[Double]]('m', "mem").optional().text("Memory for server. Optional.").action { (value, config) =>
        config.copy(mem = value)
      }

      opt[Map[String,String]]("override").valueName("Secor config override k1=v1,k2=v2...").action { (value, config) =>
        config.copy(overrides = value)
      }

      opt[Option[String]]('a', "api").optional().text("Binding host:port for http/artifact server. Optional if EM_API env is set.").action { (value, config) =>
        config.copy(api = value)
      }

    }

    val delete : OptionParser[Map[String, String]] = new CliOptionParser("delete") {

      opt[Int]('i', "id").required().text(s"CPUs for server. Required.").action { (value, config) =>
        config.updated("id", value.toString)
      }

      opt[String]('a', "api").optional().text("Binding host:port for http/artifact server. Optional if EM_API env is set.").action { (value, config) =>
        config.updated("api", value)
      }
    }

    val status = defaultParser("status")

    private def defaultParser(descr: String): OptionParser[Map[String, String]] = new CliOptionParser(descr) {
      opt[String]('a', "api").optional().text("Binding host:port for http/artifact server. Optional if EM_API env is set.").action { (value, config) =>
        config.updated("api", value)
      }
    }
  }

  class CliOptionParser(descr: String) extends OptionParser[Map[String, String]](descr) {
    override def showUsage {
      Cli.out.println(usage)
    }
  }

  case class CliError(message: String) extends RuntimeException(message)

}