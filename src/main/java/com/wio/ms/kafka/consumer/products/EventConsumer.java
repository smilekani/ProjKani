/**
 * @author Kanimozhi.M
 *
 * @Dated 18-Jun-2018  18:51:30 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.EventConsumer.java
 *
 *
 */
package com.wio.ms.kafka.consumer.products;

/**
 * @author Kanimozhi.M
 *
 * @Dated 18-Jun-2018  18:51:30 PM
 *
 */
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.framework.IProtobufMessage;
import com.wio.common.protobuf.generated.WIOEventProto.WIOEVENT;
import com.wio.ms.kafka.consumer.factory.WioConsumer;
import com.wio.ms.kafka.proto.EventProtoProcessing;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class EventConsumer extends WioConsumer 
{
	private static final Logger logger = Logger.getLogger(EventConsumer.class);

	public EventConsumer(String groupId, String topic, KafkaConsumer<byte[], byte[]> consumer) 
	{
		super(groupId, topic, consumer);
	}

	@Override
	public void processRecords(IMessageSerializer protocolType, ConsumerRecord<byte[], byte[]> record) 
	{
		logger.info("Started Consumer processing of the Event msgs.....");
		long startTime = System.currentTimeMillis();
		IProtobufMessage proto = (IProtobufMessage) protocolType.getMesgSerializer(WioKafkaConstants.WIO_EVENT_PROTO);
		WIOEVENT deserialEventObj = (WIOEVENT) proto.deSerialize(record.value());
		logger.info("Deserialized Event Object : "+deserialEventObj);
		EventProtoProcessing.getInstance().process(deserialEventObj);
		long endTime = System.currentTimeMillis();
		long timeTaken = endTime - startTime;
		logger.info("Time Taken by EventConsumer to process the Event is  : "+timeTaken);
	}

}
