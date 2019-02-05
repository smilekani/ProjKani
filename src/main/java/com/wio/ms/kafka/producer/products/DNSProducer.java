/**
 * @author Kanimozhi.M
 *
 * @Dated 19-Jul-2018  12:16:40 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.DNSProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.products;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.wio.common.protobuf.impl.WioDNSRequestProtoData;
import com.wio.ms.kafka.producer.factory.WioProducer;
import com.wio.ms.kafka.producer.generate.protobuf.DNSProtoBufProducer;

public class DNSProducer extends WioProducer {

	private static final Logger logger = Logger.getLogger(DNSProducer.class);
	
	public DNSProducer(String topic, KafkaProducer<byte[], byte[]> producer) {
		super(topic, producer);
	}

	@Override
	protected void sendMsg(String topic, KafkaProducer<byte[], byte[]> producer, int dataOrder) {	
		DNSProtoBufProducer jmsMessageProducer = new DNSProtoBufProducer();
		WioDNSRequestProtoData proto = jmsMessageProducer.constructProtoBufMsg();

		final byte[] message = proto.getWioLogByteString().toByteArray();
		logger.info("message : "+message);
		
		producer.send(new ProducerRecord<byte[], byte[]>(topic, message), new KafkaCallback());
	}

}
