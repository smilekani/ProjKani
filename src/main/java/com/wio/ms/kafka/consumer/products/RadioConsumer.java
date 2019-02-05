/**
 * @author Kanimozhi.M
 *
 * @Dated 22-Jun-2018  16:02:06 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.RadioConsumer.java
 *
 *
 */
package com.wio.ms.kafka.consumer.products;

/**
 * @author Kanimozhi.M
 *
 * @Dated 22-Jun-2018  16:02:06 PM
 *
 */
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.wio.common.protobuf.framework.IMessage;
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.generated.WIORadioProtoNew.WIORADIO;
import com.wio.ms.kafka.consumer.factory.WioConsumer;
import com.wio.ms.kafka.proto.RadioProtoProcessing;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class RadioConsumer extends WioConsumer
{
	private static final Logger logger = Logger.getLogger(RadioConsumer.class);

	public RadioConsumer(String groupId, String topic, KafkaConsumer<byte[], byte[]> consumer) 
	{
		super(groupId, topic, consumer);
	}

	@Override
	public void processRecords(IMessageSerializer protocolType, ConsumerRecord<byte[], byte[]> record) 
	{
		logger.debug("Started Consumer processing of the Radio msgs.....");
		long startTime = System.currentTimeMillis();
		IMessage proto = (IMessage) protocolType.getMesgSerializer(WioKafkaConstants.RADIO_METRIC);
		logger.info(Thread.currentThread().getName() +"| Recieved "+proto.schema() + " message."+"value : "+record.value() +
				"rec partition = "+record.partition() + "rec offset = " + record.offset());
		WIORADIO wioRadio = (WIORADIO) proto.deSerialize(record.value());
		logger.info("Deserialized wioRadio : "+wioRadio);
		RadioProtoProcessing.getInstance().process(wioRadio);
		long endTime = System.currentTimeMillis();
		long timeTaken = endTime - startTime;
		logger.info("Time Taken by RadioConsumer to process the Radio is  : "+timeTaken);
	}

}
