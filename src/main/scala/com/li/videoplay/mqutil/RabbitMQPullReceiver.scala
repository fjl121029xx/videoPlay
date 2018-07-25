package com.li.videoplay.mqutil

import com.rabbitmq.client.Channel

import scala.util.Either

class RabbitMQPullReceiver(channel: Channel, exchangeName: String) {

  def receiveMessage(): Either[Throwable, (String, Long)] = {
    try {
      val response = channel.basicGet(exchangeName, false)
      if (response == null) {
        Right((null, -10312))
      } else {
        val msg = new String(response.getBody())
        val deliveryTag = response.getEnvelope().getDeliveryTag()
        Right((msg, deliveryTag))
      }
    } catch {
      case t: Throwable => Left(t)
    }

  }

  def basicAck(deliveryTag: Long) = {
    try {
      channel.basicAck(deliveryTag, false);
      true
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        false
    }
  }

  def close() {
    try {
      if (channel != null) {
        channel.close();
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace()
    }
  }
}
