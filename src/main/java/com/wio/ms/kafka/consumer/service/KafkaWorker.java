/**
s * @author Kanimozhi.M
 *
 * @Dated 13-Jun-2018  16:32:56 PM 
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.KafkaWorker.java
 *
 *
 */
package com.wio.ms.kafka.consumer.service;

/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jun-2018  16:32:56 PM
 *
 */
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.wio.ms.kafka.consumer.factory.ConsumerFactory;
import com.wio.ms.kafka.consumer.factory.WioConsumer;
import com.wio.ms.kafka.utils.KafkaConnectionUtils;

public class KafkaWorker 
{
	private static final Logger logger = Logger.getLogger(KafkaWorker.class);

	public static void startConsumer(String groupId, String topic)
	{
		KafkaConsumer<byte[], byte[]> kafkaConsumer = KafkaConnectionUtils.getKafkaConsumer();
		int numConsumers = kafkaConsumer.partitionsFor(topic).size();
		logger.info("No. of Kafka Consumers to be Created w.r.to Partitions : "+numConsumers);
		final ExecutorService executorService = Executors.newFixedThreadPool(numConsumers);
		final List<WioConsumer> kafKaConsumers = new ArrayList<>();
		
		IntStream.range(0, numConsumers).parallel().forEach(i ->
		{
			eachConsumerAction(groupId, topic, executorService, kafKaConsumers);
			// TODO To see all the All the scenarios.
			//			newkafkaConsumer.commitAsync();
		});

		Runtime.getRuntime().addShutdownHook(new Thread() 
		{
			@Override
			public void run() 
			{
				for (WioConsumer consumerloop : kafKaConsumers) 
				{
					consumerloop.shutdown();
				} 
				executorService.shutdown();
				try 
				{
					executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
				}
				catch (InterruptedException e) 
				{
					logger.error("Exception while Executing the Consumers in the Kafka,"+e.getMessage() +e);
					e.printStackTrace();
				}
			}
		});
	}

	private static void eachConsumerAction(String groupId, String topic, final ExecutorService executorService,
			final List<WioConsumer> kafKaConsumers) {
		KafkaConsumer<byte[], byte[]> newkafkaConsumer = KafkaConnectionUtils.getKafkaConsumer();
		WioConsumer consumerObject = ConsumerFactory.getConsumer(groupId, topic, newkafkaConsumer);
		kafKaConsumers.add(consumerObject);
		executorService.submit(consumerObject);
	}

}
