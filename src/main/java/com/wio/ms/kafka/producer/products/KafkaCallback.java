/**
 * @author Kanimozhi.M
 *
 * @Dated 14-Aug-2018  2:43:11 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producer.products.KafkaCallback.java
 *
 *
 */
package com.wio.ms.kafka.producer.products;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

/**
 * @author Kanimozhi.M
 *
 * @Dated 14-Aug-2018  2:43:11 PM
 *
 */
public class KafkaCallback implements Callback {

	private static final Logger logger = Logger.getLogger(KafkaCallback.class);

	@Override
	public void onCompletion(RecordMetadata recordMetadata, Exception e) 
	{
		if ( e == null )
		{
			logger.info("Partition: "+recordMetadata.partition()
			+", Offset" + recordMetadata.offset()
			+ ", timestamp: " + recordMetadata.timestamp());
		}
		else 
		{
			e.printStackTrace();
		}
	}

}
