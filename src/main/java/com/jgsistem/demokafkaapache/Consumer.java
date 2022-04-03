package com.jgsistem.demokafkaapache;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.util.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    public static void main (String[] args){
        final Logger logger = LoggerFactory.getLogger(Consumer.class);
        final String consumerGroupID = "java-group-consumer";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"java-group-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        final KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        KafkaConsumer<String, Supplier> consumer = null;
        try{
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList("test-stream-in15"));

            while (true){
                ConsumerRecords<String, Supplier> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Supplier> recdord : records){
                    logger.info("Received new record \n" +
                            "key:" + recdord.key() + ", " +
                            "Value: " + recdord.value() + ", " +
                            "Topic: " + recdord.topic() + ", " +
                            "Partition: " + recdord.partition() + ", " +
                            "Offset: " + recdord.offset() + "\n" );
                }
                consumer.commitAsync();
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            consumer.commitSync();
            consumer.close();
        }
    };
}
