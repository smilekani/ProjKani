/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.LogProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.products;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.wio.common.protobuf.impl.WioLogProtoData;
import com.wio.ms.kafka.producer.factory.WioProducer;
import com.wio.ms.kafka.producer.generate.protobuf.LogProtoBufMsgProducer;

public class LogProducer extends WioProducer 
{
	private static final Logger logger = Logger.getLogger(LogProducer.class);

	public LogProducer(String topic, KafkaProducer<byte[], byte[]> producer) {
		super(topic, producer);
	}

	@Override
	public void sendMsg(String topic, KafkaProducer<byte[], byte[]> producer, int dataOrder) 
	{	
		LogProtoBufMsgProducer jmsMessageProducer = new LogProtoBufMsgProducer();
		WioLogProtoData proto = jmsMessageProducer.constructProtoBufMsg();

		final byte[] message = proto.getWioLogByteString().toByteArray();
		logger.info("message : "+message);
		producer.send(new ProducerRecord<byte[], byte[]>(topic, message), new KafkaCallback());
	}
}
