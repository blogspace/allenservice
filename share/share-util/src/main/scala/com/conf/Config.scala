package com.conf

import java.util.Properties

import com.typesafe.config.ConfigFactory

object Config {
  def prop(properties:String): Properties = {
    val props = new Properties()
    val in = this.getClass.getClassLoader.getResourceAsStream(properties)
    props.load(in)
    props
  }

  def prop()={
    // 通过ConfigFactory来获取到配置文件,默认加载配置文件的顺序是：application.conf --> application.json --> application.properties
    val config =  ConfigFactory.load()
    config
  }

}
