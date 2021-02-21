package com.atguigu.handler

import java.lang

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
   /*
     跨批次去重

    */
  def filterByReids(startUpLogDStream: DStream[StartUpLog]) = {

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
    val value2: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {

      //获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //过滤数据
      partition.filter(log => {
        //过滤数据
        val redisKey = "DAU:" + log.logDate
        val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
        !boolean
      })
      //归还连接
      jedisClient.close()
      partition
    })
    value2
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
