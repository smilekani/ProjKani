/**
 * @author Kanimozhi.M
 *
 * @Dated 18-Jun-2018  18:51:30 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.WioConsumer.java
 *
 *
 */
package com.wio.ms.kafka.consumer.factory;

/**
 * @author Kanimozhi.M
 *
 * @Dated 18-Jun-2018  18:51:30 PM
 *
 */
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.framework.MessageProtocolFactory;
import com.wio.ms.kafka.consumer.partition.modelling.KafkaConsumerRebalanceListener;
import com.wio.ms.kafka.utils.KafkaConfUtil;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public abstract class WioConsumer implements Runnable 
{
	private String groupId;
	private String topic;
	private KafkaConsumer<byte[], byte[]> consumer;
	AtomicBoolean canConsume = new AtomicBoolean(true);

	public WioConsumer(String groupId, String topic, KafkaConsumer<byte[], byte[]> consumer)
	{
		this.groupId = groupId;
		this.topic = topic;
		this.consumer = consumer;
	}

	@Override
	public void run() 
	{
		KafkaConsumerRebalanceListener rebalanceListener = new KafkaConsumerRebalanceListener();
		consumer.subscribe(Arrays.asList(topic), rebalanceListener);
//		consumer.subscribe(Arrays.asList(topic));
		while (canConsume.get()) 
		{
			ConsumerRecords<byte[], byte[]> records = consumer.poll(Long.MAX_VALUE);
			if(topic.equals(KafkaConfUtil.WIO_SPEEDTEST_TOPIC))
			{
				for (ConsumerRecord<byte[], byte[]> record : records) 
				{
					processJsonRecords(record);
				}
			}
			else
			{
				IMessageSerializer protocolType = MessageProtocolFactory.getFactoryInst().getMessageProtocol(WioKafkaConstants.PROTPBUF);
				for (ConsumerRecord<byte[], byte[]> record : records) 
				{
					processRecords(protocolType, record);
				}
			}
		}
	}

	public abstract void processRecords(IMessageSerializer protocolType, ConsumerRecord<byte[], byte[]> record);

	public void processJsonRecords(ConsumerRecord<byte[], byte[]> record)
	{

	}

	public void shutdown() {
		canConsume.set(false);
		consumer.wakeup();
	}


}
