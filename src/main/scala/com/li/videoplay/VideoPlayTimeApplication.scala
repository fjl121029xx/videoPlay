package com.li.videoplay

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

object VideoPlayTimeApplication {
  val checkPointinPath = "D:\\tmp\\checkpoint"
  val rabbitmqHost = "192.168.100.21"
  val rabbitmqPort = 5672
  val rabbitmqUser = "rabbitmq_ztk"
  val rabbitmaPassword = "rabbitmq_ztk"

  def main(args: Array[String]): Unit = {


    //    if (args.length != 6) {
    //      System.err.println(s"Your arguments were ${args.mkString("[", ", ", "]")}")
    //      System.err.println(
    //        """
    //          |Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>
    //          |     <output-file>. <hostname> and <port> describe the TCP server that Spark
    //          |     Streaming would connect to receive data. <checkpoint-directory> directory to
    //          |     HDFS-compatible file system which checkpoint data <output-file> file to which the
    //          |     word counts will be appended
    //          |
    //          |In local mode, <master> should be 'local[n]' with n > 1
    //          |Both <checkpoint-directory> and <output-file> must be absolute paths
    //        """.stripMargin
    //      )
    //      System.exit(1)
    //    }
    //    val Array(host, port, rabbitmqUser, rabbitmaPassword, checkPointinPath) = args

    val ssc = StreamingContext.getOrCreate(checkPointinPath,
      () => createContext(
        rabbitmqHost, rabbitmqPort.toInt, rabbitmqUser, rabbitmaPassword, checkPointinPath
      ))
    /*  val sparkConf = new SparkConf()
        .setAppName("VideoPlayTimeApplication")
        .setMaster("local[2]")

      val ssc = new StreamingContext(sparkConf, Seconds(5))
      ssc.checkpoint(checkPointinPath)

      val mqLines = ssc.receiverStream(new FanoutReceiver(ssc, rabbitmqHost, rabbitmqPort, rabbitmaPassword, rabbitmaPassword))


      val userplayTime = mqLines.map((x: String) => {

        val lineFiled = x.split("=")

        val uname = lineFiled(2)
        val recordTime = lineFiled(4)
        val userplayTime = lineFiled(3).split("\\|")(3).split(":")(1)

        (uname, userplayTime + "=" + recordTime)
      })

      userplayTime.foreachRDD(rdd => {
        rdd.foreach(t => {
          println(t._1)
          println(t._1)
        })


      })
      userplayTime.count()*/
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


  def createContext(
                     rabbitmqHost: String,
                     rabbitmqPort: Int,
                     rabbitmqUser: String,
                     rabbitmaPassword: String,
                     checkPointinPath: String): StreamingContext = {

    val sparkConf = new SparkConf()
      .setAppName("VideoPlayTimeApplication")
      .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(checkPointinPath)

    val mqLines = ssc.receiverStream(new FanoutReceiver(ssc, rabbitmqHost, rabbitmqPort, rabbitmaPassword, rabbitmaPassword))


    val userplayTime = mqLines.map((x: String) => {

      val lineFiled = x.split("=")

      val uname = lineFiled(2)
      val recordTime = lineFiled(4)
      val userplayTime = lineFiled(3).split("\\|")(3).split(":")(1)

      (uname, userplayTime + "=" + recordTime)
    })
    //todayplaytime=2018_07_20=238
    val newUpdateFunc = (seq: Seq[String], last: Option[String]) => {

      var todayplaytime: String = "2018_07_20=0"
      if (last.isDefined) {
        todayplaytime = last.get
      }

      var today = todayplaytime.split("=")(0)
      var playTime = todayplaytime.split("=")(1).toLong

      //这次输入
      val seqIte = seq.iterator


      var tmpcount: Long = 0L
      var tmptime: String = today
      while (seqIte.hasNext) {

        var value = seqIte.next()

        var utime = value.split("=")(0).toLong //用户播放视频时间
        tmpcount += utime

        var rtime = value.split("=")(1) //记录时间

        var rtimeArr = rtime.split("_")
        tmptime = rtimeArr(0) + "_" + rtimeArr(1) + "_" + rtimeArr(2)
      }


      if (today.equals(tmptime)) {
        playTime += tmpcount
      } else {
        playTime = 0L
        playTime += tmpcount
      }

      Option(tmptime + "=" + playTime)


    }

    var result = userplayTime.updateStateByKey(newUpdateFunc)
    result.print()
    result.foreachRDD(rdd => {
      val sc = rdd.context

      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "192.168.100.191")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.rootdir", "/hbase")


      val jobConf = new JobConf(conf)

      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test_tody_videoplay")

      //      sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "test_tody_videoplay")
      //
      //      val job = new Job(sc.hadoopConfiguration)
      //      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      //      job.setMapOutputValueClass(classOf[Result])
      //      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

      //user today=time
      val hbaserdd = rdd.map(t => {
        val username = t._1
        val todaytime = t._2

        val today = todaytime.split("=")(0)
        val time = todaytime.split("=")(1)

        val put = new Put(Bytes.toBytes(today + "-" + username)) //行健的值
        put.add(Bytes.toBytes("playinfo"), Bytes.toBytes("playTime"), Bytes.toBytes(time))

        (new ImmutableBytesWritable, put)
      })
      hbaserdd.saveAsHadoopDataset(jobConf)

      //      rdd.foreach(t => {
      //        println(t._1)
      //        println(t._2)
      //      })
    })

    userplayTime.count()

    ssc

  }

  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy_MM_dd")
    val date = dateFormat.format(now)
    return date
  }

}