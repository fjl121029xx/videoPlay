package com.li.videoplay

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.li.videoplay.bean.VideoPlay
import com.li.videoplay.mqutil.{RabbitMQConnHandler, RabbitMQConsumer}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

class FanoutReceiver(
                      ssm: StreamingContext,
                      rabbitmqHost: String,
                      rabbitmqPort: Int,
                      rabbitmqUsername: String,
                      rabbitmqPassword: String
                    ) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Serializable {

  private val exchangeName = "videoplay_record"
  private val queueName = "log-fb4"
  private var mqThread: Unit = null;
  val mapper = new ObjectMapper()

  //  val logger: Logger = LogManager.getLogger(this.getClass)
  val logger: Log = LogFactory.getLog(classOf[FanoutReceiver])

  override def onStart(): Unit = {
    mapper.registerModule(DefaultScalaModule)
    mqThread = new Thread(new Runnable {
      override def run(): Unit = {

        receive
      }
    }).start()
  }


  override def onStop(): Unit = {

  }

  private def receive(): Unit = {

    val mqHandler = new RabbitMQConnHandler(rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword)

    var fanoutChannnel = mqHandler.getFanoutDeclareChannel(exchangeName)
    //    val my = new MyLog4j2

    val consumer = new RabbitMQConsumer(fanoutChannnel, queueName)

    while (true) {

      //      try {
      val r = consumer.receiveMessage()
      if (r.isRight) {
        val (msg, deliveryTag) = r.right.get
        if (deliveryTag > 0) {
          val message = msg.split("=")

//          val me = message(3).replace("{","").replace("}","").replace(",","|")


//          val me2 = message(3).replace("{","").replace("}","").replace("|",":")

          //          println(message(3).contains("\""))
//          println(me)

          val obj = mapper.readValue( message(3) , classOf[VideoPlay])
          val str = message(0) + "=" + message(1) + "=" + message(2) + "=" + obj.toString + "=" + message(4)
          store(str)

          val date = message(4).split("_");

          val recordYear = date(0)
          val recordMonth = date(1)
          val recordDay = date(2)
          val recordHour = date(3)
          val recordMinute = date(3) + date(4)


          val log = message(0) + "," +
            message(1) + "," +
            message(2) + "," +
            obj.show + "," +
            recordYear + "," +
            recordMonth + "," +
            recordDay + "," +
            recordHour + "," +
            recordMinute

          logger.info("FanoutReceiver-233," + log)
          //          my.doStuff(str)
          consumer.basicAck(deliveryTag)
        } else {
          Thread.sleep(1000)
        }
      } else {
        //报错
        if (!mqHandler.connection.isOpen()) {
          mqHandler.reInitConn
        }
        if (!fanoutChannnel.isOpen()) {
          fanoutChannnel = mqHandler.getFanoutDeclareChannel(exchangeName)
        }
      }
      //      } catch {
      //        case ex: Exception => {
      //          ex.printStackTrace()
      //        }
      //      }

    }
    consumer.close()
    mqHandler.close()
  }

}

