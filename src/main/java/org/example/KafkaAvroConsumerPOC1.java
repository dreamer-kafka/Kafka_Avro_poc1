package org.example;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumerPOC1 {
    public static void main(String[] args) {
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("group.id","my-avro-consumer");
        props.put("enable.auto.commit","false");
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url","http://localhost:9081");
        props.put("specific.avro.reader","true");

        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<String, Customer>(props);
        String topic="schemacreation";
        consumer.subscribe(Collections.singleton(topic));

        System.out.println("My Data");

        while(true){
            ConsumerRecords<String,Customer> records=consumer.poll(500);
            for(ConsumerRecord<String, Customer> record:records){
                Customer customer=record.value();
                System.out.println(customer);
                //writeDataToFile(customer);
            }
            consumer.commitSync();
        }
        //consumer.close();
    }
}
