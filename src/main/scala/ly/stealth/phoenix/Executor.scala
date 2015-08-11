package ly.stealth.phoenix

import java.io.{InputStreamReader, BufferedReader, PrintWriter, StringWriter}
import java.util
import ly.stealth.phoenix.Util.Str
import org.apache.log4j._
import org.apache.mesos.{MesosExecutorDriver, ExecutorDriver}
import org.apache.mesos.Protos._
import scala.collection.JavaConverters._

object Executor extends org.apache.mesos.Executor {
  val logger: Logger = Logger.getLogger(Executor.getClass)

  def registered(driver: ExecutorDriver, executor: ExecutorInfo, framework: FrameworkInfo, slave: SlaveInfo): Unit = {
    logger.info("[registered] framework:" + Str.framework(framework) + " slave:" + Str.slave(slave))
  }

  def reregistered(driver: ExecutorDriver, slave: SlaveInfo): Unit = {
    logger.info("[reregistered] " + Str.slave(slave))
  }

  def disconnected(driver: ExecutorDriver): Unit = {
    logger.info("[disconnected]")
  }

  def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {
    logger.info("[launchTask] " + Str.task(task))
    startBroker(driver, task)
  }

  def killTask(driver: ExecutorDriver, id: TaskID): Unit = {
    logger.info("[killTask] " + id.getValue)
    stopExecutor(driver, async = true)
  }

  def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = {
    logger.info("[frameworkMessage] " + new String(data))
    handleMessage(driver, new String(data))
  }

  def shutdown(driver: ExecutorDriver): Unit = {
    logger.info("[shutdown]")
    stopExecutor(driver)
  }

  def error(driver: ExecutorDriver, message: String): Unit = {
    logger.info("[error] " + message)
  }

  def main(args: Array[String]) {

    val driver = new MesosExecutorDriver(Executor)
    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1

    System.exit(status)
  }

  private val SecorGroupProp = "-Dsecor_group=secor_backup"
  private val Log4jConfigProp = "-Dlog4j.configuration=log4j.prod.properties"
  private val ConfigProp = "-Dconfig=secor.prod.backup.properties"
  private val ClassPath = "secor-0.2-SNAPSHOT.jar:lib/*"
  private val MainClass = "com.pinterest.secor.main.ConsumerMain"


  private def startBroker(driver: ExecutorDriver, task: TaskInfo): Unit = {
    def runBroker0 {
      try {
        val data: util.Map[String, String] = Util.parseMap(task.getData.toStringUtf8).asJava
        val overrides = Util.parseMap(data.get("overrides"))

        val overrideProps = overrides.map {
          case (k, v) => s"-D$k=$v"
        }

        val cmd = Seq("java", "-ea", SecorGroupProp, Log4jConfigProp, ConfigProp) ++ overrideProps ++
          Seq("-cp", ClassPath, MainClass)

        logger.info(s"Starting process with command '$cmd'")
        val processBuilder = new ProcessBuilder(cmd: _ *)

        processBuilder.inheritIO()
        val process = processBuilder.start()

        var status = TaskStatus.newBuilder
          .setTaskId(task.getTaskId).setState(TaskState.TASK_RUNNING)
        driver.sendStatusUpdate(status.build)

        val st = process.waitFor()
        logger.info(s"Process exited with status $st")

        status = TaskStatus.newBuilder.setTaskId(task.getTaskId).setState(TaskState.TASK_FINISHED)
        driver.sendStatusUpdate(status.build)
      } catch {
        case t: Throwable =>
          logger.warn("", t)
          sendTaskFailed(driver, task, t)
      } finally {
        stopExecutor(driver)
      }
    }

    new Thread {
      override def run() {
        setName("SecorBackupNode")
        runBroker0
      }
    }.start()
  }

  private def stopExecutor(driver: ExecutorDriver, async: Boolean = false): Unit = {
    def stop0 {
      driver.stop()
    }

    if (async)
      new Thread() {
        override def run(): Unit = {
          setName("ExecutorStopper")
          stop0
        }
      }.start()
    else
      stop0
  }

  private def handleMessage(driver: ExecutorDriver, message: String): Unit = {
    if (message == "stop") driver.stop()
  }

  private def sendTaskFailed(driver: ExecutorDriver, task: TaskInfo, t: Throwable) {
    val stackTrace = new StringWriter()
    t.printStackTrace(new PrintWriter(stackTrace, true))

    driver.sendStatusUpdate(TaskStatus.newBuilder
      .setTaskId(task.getTaskId).setState(TaskState.TASK_FAILED)
      .setMessage("" + stackTrace)
      .build
    )
  }
}