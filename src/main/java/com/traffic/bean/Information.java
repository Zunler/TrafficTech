package com.traffic.bean;

public class Information {
    String date;
    String monitor_id;
    String monitor_time;
    String camera_id;
    int speed;
    String license;
    String road_id;
    String area_id;

    public Information(String date, String monitor_id, String monitor_time, String camera_id, String license, int speed, String road_id, String area_id) {
        this.date = date;
        this.monitor_id = monitor_id;
        this.monitor_time = monitor_time;
        this.camera_id = camera_id;
        this.speed = speed;
        this.license = license;
        this.road_id = road_id;
        this.area_id = area_id;
    }


}
