package com.traffic.bean

/**
  * VERSION：V1.0
  */
case class Info(date: String,
                monitor_id: String,
                camera_id: String,
                license: String,
                monitor_time: String,
                speed: Int,
                road_id: String,
                area_id: String) extends Serializable
