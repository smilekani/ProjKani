/**
 * @author Kanimozhi.M
 *
 * @Dated 18-Jun-2018  18:51:30 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.FingerPrintConsumer.java
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

import com.wio.common.protobuf.framework.IMessage;
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.generated.WIOEventsDhcpProto.DHCP;
import com.wio.ms.kafka.consumer.factory.WioConsumer;
import com.wio.ms.kafka.proto.FingerPrintProtoProcessing;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class FingerPrintConsumer extends WioConsumer
{
	private static final Logger logger = Logger.getLogger(FingerPrintConsumer.class);

	public FingerPrintConsumer(String groupId, String topic, KafkaConsumer<byte[], byte[]> consumer) 
	{
		super(groupId, topic, consumer);
	}

	@Override
	public void processRecords(IMessageSerializer protocolType, ConsumerRecord<byte[], byte[]> record) 
	{
		logger.info("processing Records in Finger Print.....");
		long startTime = System.currentTimeMillis();
		IMessage proto = (IMessage) protocolType.getMesgSerializer(WioKafkaConstants.DHCP_EVENT);
		logger.info(Thread.currentThread().getName() +"| Recieved "+proto.schema() + " message.");
		DHCP dhcp = (DHCP) proto.deSerialize(record.value());
		logger.info("finger print deseerialized Object : "+dhcp);
		FingerPrintProtoProcessing.getInstance().process(dhcp);
		long endTime = System.currentTimeMillis();
		long timeTaken = endTime - startTime;
		logger.info("Time Taken by FingerPrintConsumer to process the FingerPrint is  : "+timeTaken);
	}

}
