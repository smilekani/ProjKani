/**
 * @author Kanimozhi.M
 *
 * @Dated 18-Jun-2018  18:51:30 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumer.factory.ConsumerFactory.java
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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.wio.ms.kafka.consumer.products.ConfigUpdateConsumer;
import com.wio.ms.kafka.consumer.products.DNSRequestConsumer;
import com.wio.ms.kafka.consumer.products.EventConsumer;
import com.wio.ms.kafka.consumer.products.FingerPrintConsumer;
import com.wio.ms.kafka.consumer.products.LogConsumer;
import com.wio.ms.kafka.consumer.products.RadioConsumer;
import com.wio.ms.kafka.consumer.products.SpeedTestConsumer;
import com.wio.ms.kafka.utils.KafkaConfUtil;

public class ConsumerFactory 
{
	private static final Logger logger = Logger.getLogger(ConsumerFactory.class);
	private static Map<String, String> consumerMap = new HashMap<>();

	static
	{
		if(consumerMap != null && consumerMap.size() <= 0)
			buildConsumerMap();
	}

	private static void buildConsumerMap() 
	{
		consumerMap.put(KafkaConfUtil.WIO_LOG_TOPIC, LogConsumer.class.getName());
		consumerMap.put(KafkaConfUtil.WIO_EVENT_TOPIC, EventConsumer.class.getName());
		consumerMap.put(KafkaConfUtil.WIO_RADIO_TOPIC, RadioConsumer.class.getName());
		consumerMap.put(KafkaConfUtil.WIO_FINGER_PRINT_TOPIC, FingerPrintConsumer.class.getName());
		consumerMap.put(KafkaConfUtil.WIO_SPEEDTEST_TOPIC, SpeedTestConsumer.class.getName());
		consumerMap.put(KafkaConfUtil.WIO_CONFIG_UPDATE_TOPIC, ConfigUpdateConsumer.class.getName());
		consumerMap.put(KafkaConfUtil.WIO_DNSREQUEST_TOPIC, DNSRequestConsumer.class.getName());
	}

	public static WioConsumer getConsumer(String groupId, String topic, KafkaConsumer<byte[], byte[]> newkafkaConsumer)
	{
		logger.debug("groupId : "+groupId+"  & topic : "+topic);
		WioConsumer wioConsumer = null;
		try 
		{
			Class<?> consumerClass = Class.forName(consumerMap.get(topic));
			Constructor<?> consumerConstructor = consumerClass.getConstructor(String.class, String.class, KafkaConsumer.class);
			Object object = consumerConstructor.newInstance(new Object[] { groupId, topic, newkafkaConsumer});
			wioConsumer = (WioConsumer)object;

		} 
		catch (ClassNotFoundException | NoSuchMethodException | SecurityException |
				InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) 
		{
			logger.error("Exception while getting the Consumer,"+e.getMessage()+e);
			e.printStackTrace();
		}
		return wioConsumer;
	}

}
