package com.rpcdemo

import java.util.UUID
import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._

class Worker(val host: String, val port: Int, val masterHost: String,val masterPort: Int, val memory: Int, val cores: Int)  extends Actor{

  // 生成一个Worker ID
  val workerId: String = UUID.randomUUID().toString
println("UUID:"+workerId)
  // 用来存储MasterUrl
  var masterUrl: String = _

  // 心跳时间间隔
  val heartbeat_interval: Long = 10000

  // Master的Actor
  var master: ActorSelection = _

  /**
    * 生命周期preStart方法
    * 作用：当启动Worker时，向master发送注册信息
    */
  override def preStart(): Unit = {
    // 获取Master的Actor
    master = context.actorSelection(s"akka.tcp://${Master.MASTER_SYSTEM}" +
      s"@${masterHost}:${masterPort}/user/${Master.MASTER_ACTOR}")
    master ! RegisterWorker(workerId, host, port, memory, cores)
    println("master:"+RegisterWorker(workerId, host, port, memory, cores))
  }

  /**
    * 生命周期receive方法
    * 作用：
    * 定时向Master发送心跳信息
    */
  override def receive: Receive = {
    // Worker接收到Master发送过来的注册成功的信息（masterUrl）
    case RegisteredWorker(masterUrl) => {
      this.masterUrl = masterUrl
      // 启动一个定时器, 定时的给Master发送心跳
      import context.dispatcher
      context.system.scheduler.schedule(
        0 millis, heartbeat_interval millis, self, SendHeartbeat)
    }
    case SendHeartbeat => {
      // 向Master发送心跳信息
      master ! Heartbeat(workerId)
    }
      println("Heartbeat:"+Heartbeat(workerId))
  }
}
object Worker{
  val WORKER_SYSTEM = "WorkerSystem"
  val WORKER_ACTOR = "Worker"

  def main(args: Array[String]): Unit = {
    /**
      * 通过main方法参数指定相应的
      * worker主机名、端口号，master主机名、端口号，使用的内存和核数
      */
    val host = args(0)
    val port = args(1).toInt
    val masterHost = args(2)
    val masterPort = args(3).toInt
    val memory = args(4).toInt
    val cores = args(5).toInt

    //akka配置信息
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    // 配置创建Actor需要的配置信息
    val config: Config = ConfigFactory.parseString(configStr)

    // 创建ActorSystem
    val actorSystem: ActorSystem = ActorSystem(WORKER_SYSTEM, config)

    // 用actorSystem实例创建Actor
    actorSystem.actorOf(Props(new Worker(
      host, port, masterHost, masterPort, memory, cores)), WORKER_ACTOR)

    //actorSystem.awaitTermination()
  }
}
