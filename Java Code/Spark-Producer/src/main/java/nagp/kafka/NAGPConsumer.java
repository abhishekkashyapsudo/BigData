package nagp.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class NAGPConsumer {
	private final static String TOPIC = "test-nagp-topic1";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    
    private static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                                    "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        		StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
       

        // Create the consumer using props.
        final Consumer<String, String> consumer =
                                    new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }
    
    static void runConsumer() throws InterruptedException {
        final Consumer<String, String> consumer = createConsumer();
        System.out.println("The program will run for 2 minutes.");
        long timestamp = System.currentTimeMillis();
        while (true) {
        	if( timestamp + TimeUnit.MINUTES.toMillis(2) < System.currentTimeMillis())
        		break;
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(10000));
            

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
        System.out.println("Done.");
   
    }
    
    public static void main(String... args) throws Exception {
        runConsumer();
    }
}
