/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jun-2018  16:32:56 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.utils.KafkaConnectionUtils.java
 *
 *
 */
package com.wio.ms.kafka.utils;

/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jun-2018  16:32:56 PM
 *
 */
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaConnectionUtils 
{
	private static String KAFKA_SERVER_URL = System.getenv("KAFKA_SERVER_URL");
	private static String KAFKA_SERVER_PORT = System.getenv("KAFKA_SERVER_PORT");
	private static String GROUP_ID = System.getenv("GROUP_ID");
	private static Properties props = new Properties();
	private static Properties producerProps = new Properties();

	static
	{
		populateConsumerProperties();	
		populateProducerProperties();
	}

	private static void populateConsumerProperties()
	{
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor");
		//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		//		props.put("fetch.message.max.bytes", 10000000);

	}

	private static void populateProducerProperties()
	{
		producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
		//		producerProps.put("acks", "all");
		//		producerProps.put("retries", 0);
		//		producerProps.put("batch.size", 16384);
		//		producerProps.put("linger.ms", 2);
		//		producerProps.put("buffer.memory", 1000000000);
		//		producerProps.put("message.max.bytes", 1000000000);
		//		producerProps.put("log.segment.bytes", 1000000000);
		//		producerProps.put("replica.fetch.max.bytes", 1000000000);
		producerProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	}

	public static KafkaConsumer<byte[], byte[]> getKafkaConsumer()
	{
		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
		return consumer;
	}

	public static KafkaProducer<byte[], byte[]> getKafkaProducer()
	{
		KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);
		return producer;
	}

}
