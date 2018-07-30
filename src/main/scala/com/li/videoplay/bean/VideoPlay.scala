package com.li.videoplay.bean

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class VideoPlay(
                 val rid: String,
                 val playTime: Long,
                 val wholeTime: Long,
                 val userPlayTime: Long,
                 val roomId: String,
                 val sessionId: String,
                 val videoIdWithTeacher: String,
                 val videoIdWithoutTeacher: String,
                 val joinCode: String,
                 val syllabusId: Long
               ) {
  override def toString: String =
    "rid:" + rid +
      "|playTime:" + playTime +
      "|wholeTime:" + wholeTime +
      "|userPlayTime:" + userPlayTime +
      "|roomId:" + roomId +
      "|sessionId:" + sessionId +
      "|videoIdWithTeacher:" + videoIdWithTeacher +
      "|videoIdWithoutTeacher:" + videoIdWithoutTeacher +
      "|joinCode:" + joinCode +
      "|syllabusId:" + syllabusId

  def show: String = rid +
      "," + playTime +
      "," + wholeTime +
      "," + userPlayTime +
      "," + roomId +
      "," + sessionId +
      "," + videoIdWithTeacher +
      "," + videoIdWithoutTeacher +
      "," + joinCode +
      "," + syllabusId

}


object TopicRecord {

  def main(args: Array[String]): Unit = {


    val mapper = new ObjectMapper()

    mapper.registerModule(DefaultScalaModule)

    val json = "{\"playTime\":110,\"userPlayTime\":119,\"videoIdWithTeacher\":\"10086\",\"wholeTime\":120}"


    val obj = mapper.readValue(json, classOf[VideoPlay])

    println(obj.toString)
  }
}