package com.rpcdemo

import akka.actor.{Actor,ActorSelection, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable
import scala.concurrent.duration._

class Master(val masterHost: String, val masterPort: Int) extends Actor{

  // 用来存储Worker的注册信息: <workerId, WorkerInfo>
  val idToWorker = new mutable.HashMap[String, WorkInfo]()

  // 用来存储Worker的信息，必须使用可变的HashSet
  val workers = new mutable.HashSet[WorkInfo]()

  // Worker的超时时间间隔
  val checkInterval: Long = 15000

  /**
    * 重写生命周期preStart方法
    * 作用：当Master启动时，开启定时器，定时检查超时Worker
    */
  override def preStart(): Unit = {
    // 启动定时器，定时检查超时的Worker
    import context.dispatcher
    println("self:"+self)
    println("CheckTimeOutWorker:"+CheckTimeOutWorker)
    context.system.scheduler.schedule(0 millis,checkInterval millis, self,CheckTimeOutWorker)
  }

  /**
    *  重写生命周期receive方法
    *  作用：
    *  1.接收Worker发来的注册信息
    *  2.不断接收Worker发来的心跳信息，并更新最后一次心跳时间
    *  3.过滤出超时的Worker并移除
    */
  override def receive = {

    // 接收Worker给Master发送过来的注册信息
    case RegisterWorker(id, host, port, memory, cores) => {
      println("---------"+id+" "+host+" "+ port)
      //判断改Worker是否已经注册过，已注册的不执行任何操作，未注册的将进行注册
      if (!idToWorker.contains(id)) {
        val workerInfo = new WorkInfo(id, host, port, memory, cores)

        idToWorker += (id -> workerInfo)
        workers += workerInfo

        println("一个新的Worker注册成功")

        //向Worker发送响应信息，将masterUrl返回
        sender ! RegisteredWorker(s"akka.tcp://${Master.MASTER_SYSTEM}" +
          s"@${masterHost}:${masterPort}/user/${Master.MASTER_ACTOR}")

      }
    }
    //接收Worker发来的心跳信息
    case Heartbeat(workerId) => {
      // 通过传输过来的workerId获取对应的WorkerInfo
      val workerInfo = idToWorker(workerId)
      // 获取当前时间
      val currentTime = System.currentTimeMillis()
      // 更新最后一次心跳时间
      workerInfo.lastHeartbeatTime = currentTime
      println("time:"+workerInfo.lastHeartbeatTime)
    }
    //检查超时Worker并移除
    case CheckTimeOutWorker => {
      val currentTime = System.currentTimeMillis()
      // 把超时的Worker过滤出来
      val toRemove: mutable.HashSet[WorkInfo] =
        workers.filter(w => currentTime - w.lastHeartbeatTime > checkInterval)
      // 将超时的Worker移除
      toRemove.foreach(deadWorker => {
        idToWorker -= deadWorker.id
        workers -= deadWorker
      })
    }
      println(s"当前Worker的数量: ${workers.size}")
  }
}
object Master{
  val MASTER_SYSTEM = "MasterSystem"
  val MASTER_ACTOR = "Master"

  def main(args: Array[String]): Unit = {

    val host = args(0) // 通过main方法参数制定master主机名
    val port = args(1).toInt //通过main方法参数指定Master的端口号

    //akka配置信息
    val configStr: String =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    // 配置创建Actor需要的配置信息
    val config: Config = ConfigFactory.parseString(configStr)

    // 创建ActorSystem
    val actorSystem: ActorSystem = ActorSystem(MASTER_SYSTEM, config)

    // 用actorSystem实例创建Actor
    actorSystem.actorOf(Props(new Master(host, port)), MASTER_ACTOR)

    //actorSystem.awaitTermination()
  }
}
