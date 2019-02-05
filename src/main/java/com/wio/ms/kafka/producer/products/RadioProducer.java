/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.RadioProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.products;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.wio.common.protobuf.impl.WIORadioProtoDataNew;
import com.wio.ms.kafka.producer.factory.WioProducer;
import com.wio.ms.kafka.producer.generate.protobuf.RadioProtoBufProducer;

public class RadioProducer extends WioProducer
{
	private static final Logger logger = Logger.getLogger(RadioProducer.class);
	private Object lock = new Object();
	public RadioProducer(String topic, KafkaProducer<byte[], byte[]> producer) {
		super(topic, producer);
	}

	@Override
	protected void sendMsg(String topic, KafkaProducer<byte[], byte[]> producer, int dataOrder) {	
		RadioProtoBufProducer jmsMessageProducer = new RadioProtoBufProducer();
		synchronized (lock) {
		WIORadioProtoDataNew proto = jmsMessageProducer.constructProtoBufMsg(dataOrder);
		final byte[] message = proto.getWioRadioByteString().toByteArray();
		logger.info("message : "+message);
		producer.send(new ProducerRecord<byte[], byte[]>(topic, message), new KafkaCallback());
		}
	}

}
