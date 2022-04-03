package com.jgsistem.demokafkaapache;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class Producer {
    public static void main (String[] args){
        final Logger logger = LoggerFactory.getLogger(Producer.class);
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //create the producer
        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        for (int i = 300; i <= 350; i++){
            //create the ProducerRecord
            ProducerRecord producerRecord= new ProducerRecord("test-stream-in15","key_" + i,"jg17" + i);
            //Send Data = Asynchronous
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("\nReceived record metada. \n" +
                                "Topic:" + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() + ", " +
                                "Offset:" + recordMetadata.offset() + " @ Timestamp: " + recordMetadata.timestamp() + "\n");
                    }
                    else{
                        logger.error("Error sending" , e);
                    }
                }
            });
        }
        //flush and close producer
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
