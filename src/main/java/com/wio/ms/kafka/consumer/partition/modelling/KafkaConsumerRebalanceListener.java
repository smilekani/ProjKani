/**
 * @author Kanimozhi.M
 *
 * @Dated 14-Aug-2018  12:28:49 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumer.factory.KafkaConsumerRebalanceListener.java
 *
 *
 */
package com.wio.ms.kafka.consumer.partition.modelling;

import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

/**
 * @author Kanimozhi.M
 *
 * @Dated 14-Aug-2018  12:28:49 PM
 *
 */
public class KafkaConsumerRebalanceListener implements ConsumerRebalanceListener{

	private static final Logger logger = Logger.getLogger(KafkaConsumerRebalanceListener.class);
	
	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		logger.info("Called onPartitions Revoked with partitions:" + partitions);		
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		logger.info("Called onPartitions Assigned with partitions:" + partitions);		
	}

}
