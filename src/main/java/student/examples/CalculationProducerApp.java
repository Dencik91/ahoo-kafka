package student.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculationProducerApp {
    public static void main(String[] args) {

        // settings
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.136.129:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 0);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        final String TOPICNAME = "calculations";
        final String KEY = "sample";

        // data
        String value = IntStream.rangeClosed(1, 5).mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        System.out.println(value);
        // connecting/sending
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            // create record
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPICNAME, KEY, value);
            producer.send(record);
            System.out.println("Message sent");
        }


    }
}
