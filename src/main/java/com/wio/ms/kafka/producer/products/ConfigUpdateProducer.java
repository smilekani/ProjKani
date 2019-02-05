/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.ConfigUpdateProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.products;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.wio.common.protobuf.impl.ConfigProtoData;
import com.wio.ms.kafka.producer.factory.WioProducer;
import com.wio.ms.kafka.producer.generate.protobuf.ConfigUpdateProtoBufProducer;

public class ConfigUpdateProducer extends WioProducer {

	private static final Logger logger = Logger.getLogger(ConfigUpdateProducer.class);
	
	public ConfigUpdateProducer(String topic, KafkaProducer<byte[], byte[]> producer) {
		super(topic, producer);
	}

	@Override
	protected void sendMsg(String topic, KafkaProducer<byte[], byte[]> producer, int dataOrder) 
	{	
		ConfigUpdateProtoBufProducer jmsMessageProducer = new ConfigUpdateProtoBufProducer();
		ConfigProtoData proto = jmsMessageProducer.constructProtoBufMsg();

		final byte[] message = proto.getConfigUpdateByteString().toByteArray();
		logger.info("message : "+message);
		producer.send(new ProducerRecord<byte[], byte[]>(topic, message), new KafkaCallback());
	}

}
