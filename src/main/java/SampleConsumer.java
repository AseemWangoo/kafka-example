import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SampleConsumer {
	
	public SampleConsumer() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");	
		properties.put("kafka.topic"      , "test-topic");	
		properties.put("key.deserializer"   , "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("group.id"          , "my-group");
		
		KafkaConsumer consumer = new KafkaConsumer(properties);
		consumer.subscribe(Arrays.asList(properties.getProperty("kafka.topic")));
	      	
//  	    System.out.println("Subscribed to topic " + properties.getProperty("kafka.topic"));
		System.out.println("Kafka Consumer in Java for topic (receives every 3 sec)" + properties.getProperty("kafka.topic"));
  	    
  	    while(true) {
  	    	ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
  	  	    for (ConsumerRecord record : records)  {
  	  	    	System.out.printf("partition = %s, offset = %d, key = %s, value = %s\n", 
  	  	    			record.partition(), record.offset(), record.key(), record.value());
  	  	    }
  	    }  	      	    
		
	}		
}
