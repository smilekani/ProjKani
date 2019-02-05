/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jun-2018  16:32:56 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.KafkaConsumerService.java
 *
 *
 */
package com.wio.ms.kafka.consumer.service;

import com.wio.ms.kafka.utils.KafkaConfUtil;

public class KafkaConsumerService
{
	public static void main(String[] args) 
	{
		KafkaWorker.startConsumer(KafkaConfUtil.WIO_LOG_GROUP_ID, KafkaConfUtil.WIO_LOG_TOPIC);
		KafkaWorker.startConsumer(KafkaConfUtil.WIO_RADIO_GROUP_ID, KafkaConfUtil.WIO_RADIO_TOPIC);
		KafkaWorker.startConsumer(KafkaConfUtil.WIO_CONFIG_UPDATE_GROUP_ID, KafkaConfUtil.WIO_CONFIG_UPDATE_TOPIC);
		KafkaWorker.startConsumer(KafkaConfUtil.WIO_EVENT_GROUP_ID, KafkaConfUtil.WIO_EVENT_TOPIC);
		KafkaWorker.startConsumer(KafkaConfUtil.WIO_FINGER_PRINT_GROUP_ID, KafkaConfUtil.WIO_FINGER_PRINT_TOPIC);
		KafkaWorker.startConsumer(KafkaConfUtil.WIO_SPEEDTEST_GROUP_ID, KafkaConfUtil.WIO_SPEEDTEST_TOPIC);
		KafkaWorker.startConsumer(KafkaConfUtil.WIO_DNSREQUEST_GROUP_ID, KafkaConfUtil.WIO_DNSREQUEST_TOPIC);
	}
}
