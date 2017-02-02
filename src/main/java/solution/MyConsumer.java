/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package solution;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class MyConsumer {

    // Declare a new consumer.
    private static KafkaConsumer<String,String> consumer;

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("You must specify the topic, for example /user/user01/pump:sensor ");
        }

        String topic =  args[0] ;
        System.out.println("Subscribed to : "+ topic);

        configureConsumer();

        List<String> topics = new ArrayList<>();
        topics.add(topic);
        // Subscribe to the topic.
        consumer.subscribe(topics);

        // Set the timeout interval for requests for unread messages.
        long pollTimeOut = 1000;
        while (true) {
            // Request unread messages from the topic.
            ConsumerRecords<String, String> msg = consumer.poll(pollTimeOut);
            if (msg.count() == 0) {
                System.out.println("No messages after 1 second wait.");
            } else {
                System.out.println("Read " + msg.count() + " messages");

                // Iterate through returned records, extract the value
                // of each message, and print the value to standard output.
                Iterator<ConsumerRecord<String, String>> iter = msg.iterator();
                while (iter.hasNext()) {
                    ConsumerRecord<String, String> record = iter.next();
                    System.out.println("Consuming " + record.toString());

                }
            }
        }

    }

    /* Set the value for configuration parameters.*/
    private static void configureConsumer() {
        Properties props = new Properties();
        props.put("enable.auto.commit","true");
        props.put("group.id", "mapr-workshop");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //  which class to use to deserialize the value of each message
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
    }

}
