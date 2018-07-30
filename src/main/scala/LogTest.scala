import org.apache.logging.log4j.{Level, LogManager, Logger}

object LogTestg {


  def main(args: Array[String]): Unit = {

    val my = new MyClass
    my.doStuff()
  }
}

class MyClass  {


  def doStuff(): Unit = {
    val logger: Logger = LogManager.getLogger(this.getClass)
    logger.info("Doing stuff")
    logger.info("Doing stuff")
    logger.info("Doing stuff")
    logger.info("Doing stuff")

  }
//
//  def doStuffWithLevel(level: Level): Unit = {
//    logger(level, "Doing stuff with arbitrary level")
//  }
}

