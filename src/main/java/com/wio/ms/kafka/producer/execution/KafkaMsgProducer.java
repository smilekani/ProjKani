/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.KafkaMsgProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.execution;

/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 */
import org.apache.kafka.clients.producer.KafkaProducer;

import com.wio.ms.kafka.producer.factory.ProducerFactory;
import com.wio.ms.kafka.producer.factory.WioProducer;
import com.wio.ms.kafka.utils.KafkaConnectionUtils;

public class KafkaMsgProducer 
{
	public static void startProducer(String topic)
	{
		KafkaProducer<byte[], byte[]> kafkaProducer = KafkaConnectionUtils.getKafkaProducer();
		WioProducer producer = ProducerFactory.getProducer(topic, kafkaProducer);
		producer.publishMsg();
	}
}
