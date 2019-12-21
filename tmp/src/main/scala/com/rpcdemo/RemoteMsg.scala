package com.rpcdemo

trait RemoteMsg extends Serializable{

}
// Master 向自己发送检查超时Worker的信息
case object CheckTimeOutWorker

// Worker向Master发送的注册信息
case class RegisterWorker(id: String, host: String,port: Int, memory: Int, cores: Int) extends RemoteMsg

// Master向Worker发送的响应信息
case class RegisteredWorker(masterUrl: String) extends RemoteMsg

// Worker向Master发送的心跳信息
case class Heartbeat(workerId: String) extends RemoteMsg

// Worker向自己发送的要执行发送心跳信息的消息
case object SendHeartbeat
