package com.li.videoplay

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.li.videoplay.bean.VideoPlay
import com.li.videoplay.mqutil.{RabbitMQConnHandler, RabbitMQConsumer}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver

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


    val consumer = new RabbitMQConsumer(fanoutChannnel, queueName)

    while (true) {
      val r = consumer.receiveMessage()
      if (r.isRight) {
        val (msg, deliveryTag) = r.right.get
        if (deliveryTag > 0) {
          val message = msg.split("=")
          val obj = mapper.readValue(message(3).toString, classOf[VideoPlay])
          val str = message(0) + "=" + message(1) + "=" + message(2) + "=" + obj.toString + "=" + message(4)
          store(str)

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
    }
    consumer.close()
    mqHandler.close()
  }

}