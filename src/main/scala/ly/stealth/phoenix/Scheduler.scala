package ly.stealth.phoenix

import java.io.File
import java.util
import java.util.UUID

import com.google.protobuf.ByteString
import ly.stealth.phoenix.Util.Str
import org.apache.log4j.Logger
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

import scala.collection.JavaConverters._

class Scheduler(val config: Config) extends org.apache.mesos.Scheduler {
  private val logger = Logger.getLogger(this.getClass)

  private var driver: SchedulerDriver = null

  var cluster = new Cluster

  def start() {
    logger.info(s"Starting ${getClass.getSimpleName}")

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser("")
    frameworkBuilder.setName(config.FrameworkName)
    frameworkBuilder.setFailoverTimeout(30 * 24 * 3600)
    frameworkBuilder.setCheckpoint(true)

    val driver = new MesosSchedulerDriver(this, frameworkBuilder.build, config.MesosMaster)

    val httpServer = new HttpServer(this)
    httpServer.start()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        if (driver != null) {
          logger.info("Stopping driver")
          driver.stop()
        }
      }
    })

    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    System.exit(status)
  }

  override def registered(driver: SchedulerDriver, id: FrameworkID, master: MasterInfo) {
    logger.info("[registered] framework:" + Str.id(id.getValue) + " master:" + Str.master(master))

    this.driver = driver
  }

  override def offerRescinded(driver: SchedulerDriver, id: OfferID) {
    logger.info("[offerRescinded] " + Str.id(id.getValue))
  }

  override def disconnected(driver: SchedulerDriver) {
    logger.info("[disconnected]")

    this.driver = null
  }

  override def reregistered(driver: SchedulerDriver, master: MasterInfo) {
    logger.info("[reregistered] master:" + Str.master(master))

    this.driver = driver
  }

  override def slaveLost(driver: SchedulerDriver, id: SlaveID) {
    logger.info("[slaveLost] " + Str.id(id.getValue))
  }

  override def error(driver: SchedulerDriver, message: String) {
    logger.info("[error] " + message)
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    logger.info("[statusUpdate] " + Str.taskStatus(status))

    onServerStatus(driver, status)
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {
    logger.info("[frameworkMessage] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " data: " + new String(data))
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {
    logger.debug("[resourceOffers]\n" + Str.offers(offers.asScala))

    onResourceOffers(offers.asScala.toList)
  }

  private def onServerStatus(driver: SchedulerDriver, status: TaskStatus) {
    val server = cluster.getServerByTaskId(status.getTaskId.getValue)

    status.getState match {
      case TaskState.TASK_RUNNING =>
        onServerStarted(server, driver, status)
      case TaskState.TASK_LOST | TaskState.TASK_FAILED | TaskState.TASK_ERROR =>
        onServerFailed(server, status)
      case TaskState.TASK_FINISHED | TaskState.TASK_KILLED => logger.info(s"Task ${status.getTaskId.getValue} has finished")
      case _ => logger.warn("Got unexpected task state: " + status.getState)
    }
  }

  private def onServerStarted(serverOpt: Option[Server], driver: SchedulerDriver, status: TaskStatus) {
    serverOpt match {
      case Some(server) if server.state != State.Running =>
        logger.info(s"Changing server's (id=${server.id}) state ${server.state} -> ${State.Running}")
        server.state = State.Running
      case Some(server) => //status update for a running server
      case None =>
        logger.info(s"Got ${status.getState} for unknown/stopped server, killing task ${status.getTaskId}")
        driver.killTask(status.getTaskId)

    }
  }

  private def onServerFailed(serverOpt: Option[Server], status: TaskStatus) {
    serverOpt match {
      case Some(server) =>
        logger.info(s"Changing server's (id=${server.id}) state ${server.state} -> ${State.Stopped}")
        server.state = State.Stopped
      case None => logger.info(s"Got ${status.getState} for unknown/stopped server with task ${status.getTaskId}")
    }
  }

  private def onResourceOffers(offers: List[Offer]) {
    offers.foreach { offer =>
      val resourceOffer = ResourceOffer.fromMesosOffer(offer)
      cluster.applicableRequest(resourceOffer) match {
        case Some(server) =>
          logger.info(s"Accepting offer $offer to satisfy ${server.requestTemplate}")
          launchTask(offer, server)
        case None =>
          logger.debug(s"Declining offer ${offer.getId.getValue}")
          driver.declineOffer(offer.getId)
      }
    }
  }

  private def launchTask(offer: Offer, server: Server) {
    val mesosTask = newTask(offer, server.requestTemplate)
    val attributes = offer.getAttributesList.asScala.toList.filter(_.hasText).map(attr => attr.getName -> attr.getText.getValue).toMap
    val taskData = TaskData(mesosTask.getTaskId.getValue, mesosTask.getSlaveId.getValue, mesosTask.getExecutor.getExecutorId.getValue, attributes,
    server.requestTemplate.cpus.getOrElse(0.5), server.requestTemplate.mem.getOrElse(256))
    server.taskData = Some(taskData)

    driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(mesosTask))
    cluster.requestSatisfied(server)

    logger.info(s"Launching task $taskData for offer ${offer.getId.getValue}")
  }

  private def newTask(offer: Offer, request: RequestTemplate): TaskInfo = {
    def taskData: ByteString = {
      val requiredProperties = Map(
        "secor.kafka.topic_filter" -> config.SecorKafkaTopicFilter,
        "aws.access.key" -> config.AwsAccessKey,
        "aws.secret.key" -> config.AwsSecretKey,
        "secor.s3.bucket" -> config.SecorS3Bucket,
        "kafka.seed.broker.host" -> config.KafkaSeedBrokerHost,
        "zookeeper.quorum" -> config.ZookeeperQuorum,
        "secor.local.path" -> "./")

      val data = Map("overrides" -> Util.formatMap(requiredProperties ++ request.configOverrides))

      ByteString.copyFromUtf8(Util.formatMap(data))
    }

    val taskBuilder: TaskInfo.Builder = TaskInfo.newBuilder
      .setName("task-" + UUID.randomUUID())
      .setTaskId(TaskID.newBuilder.setValue(UUID.randomUUID().toString).build)
      .setData(taskData)
      .setSlaveId(offer.getSlaveId)
      .setExecutor(newExecutor(request.id))

    taskBuilder
      .addResources(Resource.newBuilder.setName("cpus").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder.setValue(request.cpus.getOrElse(0.2))))
      .addResources(Resource.newBuilder.setName("mem").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder.setValue(request.mem.getOrElse(256))))

    taskBuilder.build
  }

  private def newExecutor(serverId: Int): ExecutorInfo = {
    val cmd = "java -cp phoenix-0.1-SNAPSHOT.jar ly.stealth.phoenix.Executor"

    val commandBuilder = CommandInfo.newBuilder
    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(new File("dist/phoenix-0.1-SNAPSHOT.jar").getAbsolutePath))
      .addUris(CommandInfo.URI.newBuilder().setValue(new File("dist/secor-0.2-SNAPSHOT-bin.tar.gz").getAbsolutePath).setExtract(true))
      .setValue(cmd)

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder.setValue(serverId.toString))
      .setCommand(commandBuilder)
      .setName(s"secor-$serverId")
      .build()
  }


  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {
    logger.info("[executorLost] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " status:" + status)
  }

  def onAddServer(request: AddServerRequest): Unit = {
    logger.info(s"Handling $request")
    cluster.addRequest(request.asRequestTemplate)
  }

  def onDeleteServer(request: DeleteServerRequest): Unit = {
    logger.info(s"Handling $request")
    val taskIdOpt = cluster.getServer(request.id).flatMap(_.taskData.map(_.id))
    taskIdOpt.foreach(taskId => driver.killTask(TaskID.newBuilder().setValue(taskId).build()))
    cluster.deleteServer(request.id)
  }
}