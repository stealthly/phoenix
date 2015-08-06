package ly.stealth.phoenix

import java.io.File

object Cli {
  def main(args:Array[String]): Unit = {
    val config = Config(new File(args(0)))

    val scheduler = new Scheduler(config)

    scheduler.start()
  }
}
