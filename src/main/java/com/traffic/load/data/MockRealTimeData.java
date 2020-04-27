package com.traffic.load.data;

import com.google.gson.Gson;
import com.traffic.bean.Information;
import com.traffic.util.DateUtils;
import com.traffic.util.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * 模拟实时的数据
 *
 * @author leelovejava
 * @date 2019-07-31
 */
public class MockRealTimeData extends Thread {

    private static final Random random = new Random();
    private static final String[] locations = new String[]{"鲁", "京", "京", "京", "沪", "京", "京", "深", "京", "京"};
    private KafkaProducer<String, String> producer;


    public MockRealTimeData() {
        producer = new KafkaProducer<String,String>(createProducerConfig());
    }

    private Properties createProducerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.29.106:9092,172.16.29.107:9092,172.16.29.108:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public void run() {
        System.out.println("正在生产数据...");
        while (true) {

            String date = DateUtils.getTodayDate();
            String baseActionTime = date + " " + StringUtils.fulFuill(random.nextInt(24) + "");
            baseActionTime = date + " " + StringUtils.fulFuill((Integer.parseInt(baseActionTime.split(" ")[1]) + 1) + "");
            String actionTime = baseActionTime + ":" + StringUtils.fulFuill(random.nextInt(60) + "") + ":" + StringUtils.fulFuill(random.nextInt(60) + "");
            String monitorId = StringUtils.fulFuill(4, random.nextInt(9) + "");
            String car = locations[random.nextInt(10)] + (char) (65 + random.nextInt(26)) + StringUtils.fulFuill(5, random.nextInt(99999) + "");
            int speed = Integer.parseInt(random.nextInt(260) + "");
            String roadId = random.nextInt(50) + 1 + "";
            String cameraId = StringUtils.fulFuill(5, random.nextInt(9999) + "");
            String areaId = StringUtils.fulFuill(2, random.nextInt(8) + "");
            Information info = new Information(date,monitorId,actionTime,cameraId,car,speed,roadId,areaId);
            Gson gson= new Gson();
            producer.send(new ProducerRecord<String,String>("RoadRealTimeLog", gson.toJson(info)));
            System.out.println("：："+date + "\t" + monitorId + "\t" + cameraId + "\t" + car + "\t" + actionTime + "\t" + speed + "\t" + roadId + "\t" + areaId);
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
