package com.xyzxc.Kafka.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xyzxc.Kafka.producer.ProducerDemo;

public class ConsumerDemo {
	private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

	public static void main(String[] args) {
		log.info("Kafka Consumer Demo");
		
		String groupId="my-java-application";
		
		String topic ="demo_java";
		
		
		

		Properties properties = new Properties();
		properties.put("bootstrap.servers", "127.0.0.1:9092");

		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());

		properties.setProperty("group.id", groupId);
		
		properties.setProperty("auto.offset.reset", "earliest");
		
		
		KafkaConsumer<String,String> consumer=new KafkaConsumer<>(properties);
		
		List<String> topics=new ArrayList<String>();
		topics.add(topic);
		
		
		consumer.subscribe(topics);
		
		
		while(true) {
			log.info("Polling");
			
			ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(1000));
			
			
			for(ConsumerRecord<String, String> record:records) {
				
				log.info("Key:"+record.key()+",Value:"+record.value());
				log.info("Partition:" + record.partition() + ",Offset:" + record.offset());
			}
		}
	}
}
