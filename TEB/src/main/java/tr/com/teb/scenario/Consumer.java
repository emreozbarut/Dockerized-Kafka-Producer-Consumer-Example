package tr.com.teb.scenario;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {
    private final Logger messageLogger = LoggerFactory.getLogger(Consumer.class.getName());
    private final String messageBootstrapServer;
    private final String messageGroupId;
    private final String messageTopic;

    Consumer(String bootstrapServer, String groupId, String topic) {
        messageBootstrapServer = bootstrapServer;
        messageGroupId = groupId;
        messageTopic = topic;
    }

    public static void main(String[] args) {
        String server = "127.0.0.1:9092";
        String groupId = "some_application";
        String topic = "user_registered";

        new Consumer(server, groupId, topic).run();
    }

    void run() {
        messageLogger.info("Creating consumer thread");

        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable runnable = new ConsumerRunnable(messageBootstrapServer, messageGroupId, messageTopic, latch);
        Thread thread = new Thread(runnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            messageLogger.info("Caught shutdown hook!");
            runnable.shutdown();
            await(latch);

            messageLogger.info("Application has exited!");
        }));

        await(latch);
    }

    void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            messageLogger.error("Application got interrupted", e);
        } finally {
            messageLogger.info("Application is closing");
        }
    }

    private class ConsumerRunnable implements Runnable {
        private CountDownLatch messageLatch;
        private KafkaConsumer<String, String> messageConsumer;

        ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
            messageLatch = latch;

            Properties props = consumerProps(bootstrapServer, groupId);
            messageConsumer = new KafkaConsumer<String, String>(props);
            messageConsumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                do {
                    ConsumerRecords<String, String> records = messageConsumer.poll(Duration.ofMillis(10));

                    for (ConsumerRecord<String, String> record : records) {
                        messageLogger.info("Key: " + record.key() + ", Value: " + record.value());
                        messageLogger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                } while (true);
            } catch (WakeupException we) {
                messageLogger.info("Received shutdown signal!");
            } finally {
                messageConsumer.close();
                messageLatch.countDown();
            }
        }

        private Properties consumerProps(String bootstrapServer, String groupId) {
            String deserializer = StringDeserializer.class.getName();
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            return props;
        }

        void shutdown() {
            messageConsumer.wakeup();
        }
    }
}
