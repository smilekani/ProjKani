/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.SpeedTestProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.products;

/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 */
import java.io.IOException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.wio.ms.kafka.producer.factory.WioProducer;
import com.wio.ms.kafka.producer.generate.protobuf.SpeedTestProtoBufProducer;

public class SpeedTestProducer extends WioProducer
{
	private static final Logger logger = Logger.getLogger(SpeedTestProducer.class);

	public SpeedTestProducer(String topic, KafkaProducer<byte[], byte[]> producer) {
		super(topic, producer);
	}

	@Override
	protected void sendMsg(String topic, KafkaProducer<byte[], byte[]> producer, int dataOrder) {	
		SpeedTestProtoBufProducer jmsMessageProducer = new SpeedTestProtoBufProducer();
		try {
			final byte[] message = jmsMessageProducer.constructProtoBufMsg().getBytes();
			logger.info("message : "+message);
			producer.send(new ProducerRecord<byte[], byte[]>(topic, message), new KafkaCallback());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}
