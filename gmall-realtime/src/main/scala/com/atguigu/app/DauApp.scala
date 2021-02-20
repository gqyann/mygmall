package com.atguigu.app

import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //获取Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    kafkaDStream.foreachRDD(rdd => {
      rdd.foreach(record => {
        println(record.value())
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
