package com.xyzxc.Kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	
private static final Logger log= LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
	
	public static void main(String[] args) {
		log.info("Kafka Producer Demo");
		
		Properties properties=new Properties();
		properties.put("bootstrap.servers", "127.0.0.1:9092");
		
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
		
		KafkaProducer<String,String> producer=new KafkaProducer<>(properties);
		
		for(int i=0;i<10;i++) {
			
			ProducerRecord<String,String> producerRecord=new ProducerRecord<>("demo_java","hello world_"+i);
			
		
			producer.send(producerRecord,new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// TODO Auto-generated method stub
					
					if(exception==null) {
						log.info("Receive metatdata \n"+
					"Topic:"+metadata.topic()+"\n"+
					"Partition:"+metadata.partition()+"\n"+
					"Offset:"+metadata.offset()+"\n"+
					"Timestamp:"+metadata.timestamp());
					}
					else {
						log.error("Error while producing",exception);
					}
					
				}
			});
			
		
		}
		
	
		producer.flush();
		
		producer.close();
	}

}
