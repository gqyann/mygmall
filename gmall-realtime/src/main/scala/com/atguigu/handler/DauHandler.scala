package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 批次内去重
    * @param filterByRedisDStream
    */
  def filterByMid(filterByRedisDStream: DStream[StartUpLog]) = {
    //1. 转换数据结构
    val mid2LogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => {((log.mid, log.logDate), log)})

    //2. groupbyKey
    val mid2LogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = mid2LogDStream.groupByKey()

    //3. 对value取第一个
    val mid2LogList: DStream[((String, String), List[StartUpLog])] = mid2LogIterDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //4. 扁平化
    val result: DStream[StartUpLog] = mid2LogList.flatMap(_._2)
    result
  }

  /*
    跨批次去重
   */
  //  def filterByReids(startUpLogDStream: DStream[StartUpLog]) = {
  def filterByReids(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {

    //方案1
    /*val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      //获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //过滤数据
      val redisKey = "DAU:" + log.logDate
      val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)

      //归还连接
      jedisClient.close()

      !boolean
    })
    value
  }*/

    // 方案2（在分区下获取连接，减少连接个数）
    /*val value2: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {

      //获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //过滤数据
      partition.filter(log => {
        //过滤数据
        val redisKey = "DAU:" + log.logDate
        !jedisClient.sismember(redisKey, log.mid)
      })
      //归还连接
      jedisClient.close()
      partition
    })
    value2*/

    // 方案3(每个批次获取一次连接)
    startUpLogDStream.transform(rdd => {
      //1.获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //2.获取数据
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val date: String = sdf.format(System.currentTimeMillis())
      val redisKey = "DAU:" + date
      val midSet: util.Set[String] = jedisClient.smembers(redisKey)

      //3.关闭连接
      jedisClient.close()

      //4.广播数据
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(midSet)

      //5.过滤数据
      val value3: RDD[StartUpLog] = rdd.filter(log => {
        !midBC.value.contains(log.mid)
      })
      value3
    })
  }


  /*
    将去重后的数据写入redis

   */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {

      rdd.foreachPartition(partition => {
        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        partition.foreach(log => {
          //2. 写库
          val redisKey = "DAU:" + log.logDate
          jedisClient.sadd(redisKey, log.mid)
        })
        //3. 释放
        jedisClient.close()
      })
    })
  }

}
