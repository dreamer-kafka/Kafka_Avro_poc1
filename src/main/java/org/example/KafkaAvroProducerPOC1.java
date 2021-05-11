package org.example;


import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducerPOC1 {
    public static void main(String[] args) {
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("ack","all");
        props.put("retries",10);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url","http://localhost:9081");
        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(props);
        String topic="schemacreation";
        Customer customer=Customer.newBuilder()
                .setFirstName("lee")
                .setLastName("min ho")
                .setCustomerId(2)
                .build();

        ProducerRecord<String,Customer> producerRecord = new ProducerRecord<String, Customer>(topic,customer
        );
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {

                if (exception == null) {
                    System.out.println("Success!");
                    System.out.println(metadata.toString());
                } else {
                    exception.printStackTrace();
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}



