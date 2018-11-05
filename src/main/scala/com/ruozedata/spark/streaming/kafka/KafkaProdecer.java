package com.ruozedata.spark.streaming.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/*
    依赖直接使用spark-kafka的集成，不要用kafka-clients
 */
public class KafkaProdecer {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 30000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 
        Producer<String, String> producer = new KafkaProducer(props);

        for (int i = 0; i < 100; i++) {
            StringBuilder sb = new StringBuilder();
            producer.send(new ProducerRecord<String, String>("test2", Integer.toString(i), sb.append("ruoze").append(i).toString()));
        }
        producer.close();
    }
}
