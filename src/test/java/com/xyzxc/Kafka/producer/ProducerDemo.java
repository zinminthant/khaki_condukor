package com.xyzxc.Kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ProducerDemo {
	
	private static final Logger log= LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
	
	public static void main(String[] args) {
		log.info("Kafka Producer Demo");
		
		Properties properties=new Properties();
		properties.put("bootstrap.servers", "127.0.0.1:9092");
		
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
		
		KafkaProducer<String,String> producer=new KafkaProducer<>(properties);
		
		
		ProducerRecord<String,String> producerRecord=new ProducerRecord<>("demo_java","hello world");
		
	
		producer.send(producerRecord);
		
	
		producer.flush();
		
		producer.close();
	}

}
