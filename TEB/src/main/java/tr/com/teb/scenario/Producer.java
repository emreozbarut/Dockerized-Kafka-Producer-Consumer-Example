package tr.com.teb.scenario;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    final KafkaProducer<String, String> messageProducer;
    final Logger messageLogger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String server = "127.0.0.1:9092";
        String topic = "user_registered";

        Producer producer = new Producer(server);
        producer.put(topic, "user1", "Yasin");
        producer.put(topic, "user2", "Emre");
        producer.close();
    }

    Producer(String bootstrapServer) {
        Properties props = producerProps(bootstrapServer);
        messageProducer = new KafkaProducer<>(props);
        messageLogger.info("Producer initialized!");
    }

    private Properties producerProps (String bootstrapServer) {
        String serializer = StringSerializer.class.getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        return props;
    }

    void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
        messageLogger.info("Put value: " + value + ", for key: " + key);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        messageProducer.send(record, (recordMetadata, e) -> {
            if(e != null) {
                messageLogger.error("Error while producing message!", e);
                return;
            }
            messageLogger.info("Received new meta, \n" +
                    "Topic: " + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp());
        });
    }

    void close () {
        messageLogger.info("Closing producer's connection!");
        messageProducer.close();
    }
}
