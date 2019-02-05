/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.ProducerFactory.java
 *
 *
 */
package com.wio.ms.kafka.producer.factory;

/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 */
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

import com.wio.ms.kafka.producer.products.ConfigUpdateProducer;
import com.wio.ms.kafka.producer.products.DNSProducer;
import com.wio.ms.kafka.producer.products.EventProducer;
import com.wio.ms.kafka.producer.products.FingerPrintProducer;
import com.wio.ms.kafka.producer.products.LogProducer;
import com.wio.ms.kafka.producer.products.RadioProducer;
import com.wio.ms.kafka.producer.products.SpeedTestProducer;
import com.wio.ms.kafka.utils.KafkaConfUtil;

public class ProducerFactory 
{
	private static final Logger logger = Logger.getLogger(ProducerFactory.class);
	private static Map<String, String> producerMap = new HashMap<>();

	static
	{
		if(producerMap != null && producerMap.size() <= 0)
			buildConsumerMap();
	}

	private static void buildConsumerMap() 
	{
		producerMap.put(KafkaConfUtil.WIO_LOG_TOPIC, LogProducer.class.getName());
		producerMap.put(KafkaConfUtil.WIO_EVENT_TOPIC, EventProducer.class.getName());
		producerMap.put(KafkaConfUtil.WIO_RADIO_TOPIC, RadioProducer.class.getName());
		producerMap.put(KafkaConfUtil.WIO_FINGER_PRINT_TOPIC, FingerPrintProducer.class.getName());
		producerMap.put(KafkaConfUtil.WIO_SPEEDTEST_TOPIC, SpeedTestProducer.class.getName());
		producerMap.put(KafkaConfUtil.WIO_CONFIG_UPDATE_TOPIC, ConfigUpdateProducer.class.getName());
		producerMap.put(KafkaConfUtil.WIO_DNSREQUEST_TOPIC, DNSProducer.class.getName());
	}

	public static WioProducer getProducer(String topic, KafkaProducer<byte[], byte[]> kafkaProducer)
	{
		logger.info("topic : "+topic);
		WioProducer wioProducer = null;
		try 
		{
			Class<?> consumerClass = Class.forName(producerMap.get(topic));
			Constructor<?> consumerConstructor = consumerClass.getConstructor(String.class, KafkaProducer.class);
			Object object = consumerConstructor.newInstance(new Object[] { topic, kafkaProducer });
			wioProducer = (WioProducer)object;

		} 
		catch (ClassNotFoundException | NoSuchMethodException | SecurityException |
				InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) 
		{
			logger.error("Exception while getting the Consumer,"+e.getMessage()+e);
			e.printStackTrace();
		}
		return wioProducer;
	}

}
