package com.liwei.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Producer implements Runnable {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public Producer(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topicName;
    }

    public void run() {
        BufferedReader bf = null;
        String data ="";
        try {
            bf = new BufferedReader(new FileReader("D:\\result.txt"));
            while (bf.readLine()!=null){
                data +=bf.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            for (; ; ) {
                producer.send(new ProducerRecord<String, String>(topic, "Message", data));
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void main(String args[]) {
        Producer test = new Producer("log3");
        Thread thread = new Thread(test);
        thread.start();
    }

}
