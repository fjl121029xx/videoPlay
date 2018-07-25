package com.li.videoplay.mqutil

import com.rabbitmq.client.{Channel, MessageProperties}

class RabbitMQProducer(channel: Channel, exchangeName: String) {

  def sendQueueMsg(
                    exchangeName: String,
                    messageBodyBytes: String) {
    channel.basicPublish("",
      exchangeName,
      MessageProperties.PERSISTENT_TEXT_PLAIN,
      messageBodyBytes.getBytes)
  }

  def sendExchangeMsg(
                       exchangeName: String,
                       routingKey: String,
                       messageBodyBytes: String) {
    channel.basicPublish(
      exchangeName,
      routingKey, null,
      messageBodyBytes.getBytes)
  }

  def close() {
    if (channel != null) {
      channel.close()
    }
  }
}
