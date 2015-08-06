package ly.stealth.phoenix

import java.io.File
import java.util
import java.util.UUID

import ly.stealth.phoenix.Util.Str
import org.apache.log4j.Logger
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

import scala.collection.JavaConverters._

class Scheduler(config: Config) extends org.apache.mesos.Scheduler {
  private val logger = Logger.getLogger(this.getClass)

  private var driver: SchedulerDriver = null

  var cluster: Cluster = new Cluster

  def start() {
    logger.info(s"Starting ${getClass.getSimpleName}")

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser("")
    frameworkBuilder.setName(config.FrameworkName)
    frameworkBuilder.setFailoverTimeout(30 * 24 * 3600)
    frameworkBuilder.setCheckpoint(true)

    val driver = new MesosSchedulerDriver(this, frameworkBuilder.build, config.MesosMaster)

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
    val node = cluster.getNode(status.getTaskId.getValue)

    status.getState match {
      case TaskState.TASK_RUNNING =>
        onServerStarted(node, driver, status)
      case TaskState.TASK_LOST | TaskState.TASK_FAILED | TaskState.TASK_ERROR =>
        onServerFailed(node, status)
      case TaskState.TASK_FINISHED | TaskState.TASK_KILLED => logger.info(s"Task ${status.getTaskId.getValue} has finished")
      case _ => logger.warn("Got unexpected task state: " + status.getState)
    }
  }

  private def onServerStarted(nodeOpt: Option[Node], driver: SchedulerDriver, status: TaskStatus) {
    nodeOpt match {
      case Some(node) =>
        node.state = State.RUNNING
      case None =>
        logger.info(s"Got ${status.getState} for unknown/stopped server, killing task ${status.getTaskId}")
        driver.killTask(status.getTaskId)
    }
  }

  private def onServerFailed(nodeOpt: Option[Node], status: TaskStatus) {
    nodeOpt match {
      case Some(node) => node.state = State.STOPPED
      case None => logger.info(s"Got ${status.getState} for unknown/stopped server with task ${status.getTaskId}")
    }
  }

  private def onResourceOffers(offers: List[Offer]) {
    offers.foreach { offer =>
      val resourceOffer = ResourceOffer.fromMesosOffer(offer)
      cluster.applicableRequest(resourceOffer) match {
        case Some(rq) =>
          logger.info(s"Accepting offer $offer to satisfy $rq")
          launchTask(offer, rq)
        case None =>
          logger.debug(s"Declining offer $offer")
          driver.declineOffer(offer.getId)
      }
    }
  }

  private def launchTask(offer: Offer, request: RequestTemplate) {
    val task = newTask(offer)
    val taskId = task.getTaskId.getValue
    val attributes = offer.getAttributesList.asScala.toList.filter(_.hasText).map(attr => attr.getName -> attr.getText.getValue).toMap

    driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(task))
    val node = Node(taskId, task.getSlaveId.getValue, task.getExecutor.getExecutorId.getValue, offer.getHostname, attributes, State.RUNNING)
    cluster.requestSatisfied(request, node)
    logger.info(s"Launching task $taskId for offer ${offer.getId.getValue}")
  }

  private def newTask(offer: Offer): TaskInfo = {
    val taskBuilder: TaskInfo.Builder = TaskInfo.newBuilder
      .setName("task-" + UUID.randomUUID())
      .setTaskId(TaskID.newBuilder.setValue(UUID.randomUUID().toString).build)
      .setSlaveId(offer.getSlaveId)
      .setExecutor(newExecutor())

    taskBuilder
      .addResources(Resource.newBuilder.setName("cpus").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder.setValue(0.5)))
      .addResources(Resource.newBuilder.setName("mem").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder.setValue(256)))

    taskBuilder.build
  }

  private def newExecutor(): ExecutorInfo = {
    val cmd = "java -cp phoenix-0.1-SNAPSHOT.jar ly.stealth.phoenix.Executor"

    val commandBuilder = CommandInfo.newBuilder
    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(new File("dist/phoenix-0.1-SNAPSHOT.jar").getAbsolutePath))
      .addUris(CommandInfo.URI.newBuilder().setValue(new File("dist/secor-0.2-SNAPSHOT-bin.tar.gz").getAbsolutePath).setExtract(true))
      .setValue(cmd)

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder.setValue(UUID.randomUUID().toString))
      .setCommand(commandBuilder)
      .setName("node-" + UUID.randomUUID())
      .build()
  }


  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {
    logger.info("[executorLost] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " status:" + status)
  }

}
