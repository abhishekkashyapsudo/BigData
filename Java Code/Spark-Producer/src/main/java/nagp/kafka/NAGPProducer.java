package nagp.kafka;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class NAGPProducer {
	private final static String TOPIC = "nagp-topic";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private static int index = 1;

	/**
	 * Setting up the configuration properties for Kafka client
	 * 
	 * @return
	 */
	private static Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, TOPIC);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	/**
	 * 
	 * @param sendMessageCount
	 * @throws Exception
	 */
	static void runProducer(String filename) throws Exception {
		final Producer<String, String> producer = createProducer();
		try (Stream<String> stream = Files.lines(Paths.get(filename))) {
			stream.forEach(word -> {
				try {
					sendMessage(producer, word);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			});
		}

		producer.flush();
		producer.close();

	}

	static void sendMessage(Producer<String, String> producer, String word)
			throws InterruptedException, ExecutionException {

		long time = System.currentTimeMillis();

		final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, time + "" + (index++), word);

		RecordMetadata metadata = producer.send(record).get();

		long elapsedTime = System.currentTimeMillis() - time;
		System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(),
				record.value(), metadata.partition(), metadata.offset(), elapsedTime);

	}

	/**
	 * call runProducer method
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		runProducer("sample.txt");
	}
}
