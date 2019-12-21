package com.rpcdemo

class WorkInfo(val id: String, val host: String, val port: Int,val memory: Int, val cores: Int) {
  // 记录最后一次心跳时间
  var lastHeartbeatTime: Long = _
}
