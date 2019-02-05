/**
 * @author Kanimozhi.M
 *
 * @Dated 18-Jun-2018  18:51:30 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.LogConsumer.java
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
import com.wio.common.protobuf.generated.WIOLogProto.LOG;
import com.wio.common.protobuf.impl.WioLogProtoData;
import com.wio.ms.kafka.consumer.factory.WioConsumer;
import com.wio.ms.kafka.proto.LogProtoProcessing;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class LogConsumer extends WioConsumer 
{
	private static final Logger logger = Logger.getLogger(LogConsumer.class);

	public LogConsumer(String groupId, String topic, KafkaConsumer<byte[], byte[]> consumer) 
	{
		super(groupId, topic, consumer);
	}

	@Override
	public void processRecords(IMessageSerializer protocolType, ConsumerRecord<byte[], byte[]> record) 
	{
		logger.debug("Started Consumer processing of the Log msgs.....");
		long startTime = System.currentTimeMillis();
		WioLogProtoData proto = (WioLogProtoData) protocolType.getMesgSerializer(WioKafkaConstants.WIO_LOG_PROTO);
		LOG deseriLogObj = (LOG) proto.deSerialize(record.value());
		logger.info("Deserialized Log Object : "+deseriLogObj);
		LogProtoProcessing.getInstance().process(deseriLogObj);
		long endTime = System.currentTimeMillis();
		long timeTaken = endTime - startTime;
		logger.info("Time Taken by LogConsumer to process the Log is  : "+timeTaken);
	}

}
