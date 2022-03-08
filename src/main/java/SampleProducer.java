import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SampleProducer {
	
    
    public SampleProducer() throws InterruptedException {
    	Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer"   , "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("kafka.topic"      , "test-topic");
		
		KafkaProducer kafkaProducer = new KafkaProducer(properties);
		int i=0;
		
		System.out.println("Kafka Producer in Java for topic (sends every 3 sec)" + properties.getProperty("kafka.topic"));
		
		try {
			while (true) {
				i++;
				ProducerRecord producerRecord = new ProducerRecord("test-topic","key","message-" +i);
				Thread.sleep(3000);
				kafkaProducer.send(producerRecord);
			}
		} finally {
			kafkaProducer.close();
		}				
    }
                  
}
