/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.WioProducer.java
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
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;

public abstract class WioProducer 
{
	private String topic;
	private KafkaProducer<byte[], byte[]> producer;
	public WioProducer(String topic, KafkaProducer<byte[], byte[]> producer)
	{
		this.topic = topic;
		this.producer = producer;
	}

	public void publishMsg()
	{		
		IntStream.range(0, 10).parallel().forEach(i -> {
			try {
				sendMsg(topic, producer, i);
				TimeUnit.MILLISECONDS.sleep(50);
			}
			catch ( Exception e   ){
				System.out.println("I was interrupted."+e);
				e.printStackTrace();
			}

		});
	}

	protected abstract void sendMsg(String topic, KafkaProducer<byte[], byte[]> producer, int dataOrder);

}
