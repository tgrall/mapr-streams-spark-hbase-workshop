/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package solution;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

public class MyProducer {

    private static int MAX_DELAY = 300;

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1) {
            throw new IllegalArgumentException("You must specify the topic, for example /user/user01/pump:sensor ");
        }

        String topic = args[0];
        System.out.println("Publishing to to : " + topic);

        KafkaProducer<String, String> producer = configureProducer();

        BufferedReader sensorData = sensorData();

        String line = sensorData.readLine();
        while (line != null) {
            String[] values = line.split(",");
            String key = values[0];
            /* Add each message to a record. A ProducerRecord object
             identifies the topic or specific partition to publish
             a message to. */
            ProducerRecord<String, String> rec = new ProducerRecord<>(topic, key, line);

            // Send the record to the producer client library.
            System.out.println("Sending to topic " + topic);
            producer.send(rec);
            System.out.println("Sent message " + line);
            line = sensorData.readLine();
            Thread.sleep(randomDelay());
        }

        producer.close();
        System.out.println("All done.");

        System.exit(1);
    }

    /**
     * Set the value for a configuration parameter.
     * This configuration parameter specifies which class
     * to use to serialize the value of each message.
     * You can find out more about the producer parameters at
     * https://kafka.apache.org/documentation/#producerconfigs
     * and
     * https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
     **/
    private static KafkaProducer<String, String> configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }

    //Boilerplate

    /**
     * Introduces a random delay when producing elements so everything doesn't
     * arrive all bunched up at the same time.
     *
     * @return a number of milliseconds between 0 and 300
     */
    private static int randomDelay() {
        return (int) (Math.random() * MAX_DELAY);
    }

    /**
     * Reads data from `./data/sensordata.csv`
     *
     * @return A BufferedReader for the sensor data
     * @throws FileNotFoundException
     */
    private static BufferedReader sensorData() throws FileNotFoundException {
        File file = new File("./data/sensordata.csv");
        FileReader fr = new FileReader(file);
        return new BufferedReader(fr);
    }
}
