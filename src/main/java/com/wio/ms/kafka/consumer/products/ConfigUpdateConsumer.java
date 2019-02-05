/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  16:15:50 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.ConfigUpdateConsumer.java
 *
 *
 */
package com.wio.ms.kafka.consumer.products;

/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  16:15:50 PM
 *
 */
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.wio.common.protobuf.framework.IMessage;
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.generated.ConfigUpdatesProto.ConfigUpdates;
import com.wio.common.protobuf.generated.ConfigUpdatesProto.ConfigUpdates.ConfigParam;
import com.wio.ms.kafka.consumer.factory.WioConsumer;
import com.wio.ms.kafka.consumers.db.ConfigFailureRespHandler;
import com.wio.ms.kafka.consumers.db.ConfigSuccessRespHandler;
import com.wio.ms.kafka.consumers.db.FirmwareDownloadFailureRespHandler;
import com.wio.ms.kafka.consumers.db.FirmwareDownloadSuccessRespHandler;
import com.wio.ms.kafka.consumers.db.FirmwareUpgradeFailureRespHandler;
import com.wio.ms.kafka.consumers.db.FirmwareUpgradeSuccessRespHandler;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class ConfigUpdateConsumer extends WioConsumer {

	private static final Logger logger = Logger.getLogger(ConfigUpdateConsumer.class);

	private List<ConfigParam> configParam = null;

	public ConfigUpdateConsumer(String groupId, String topic, KafkaConsumer<byte[], byte[]> consumer) {
		super(groupId, topic, consumer);
	}

	@Override
	public void processRecords(IMessageSerializer protocolType, ConsumerRecord<byte[], byte[]> record) {
		logger.info("Processing Records in Config Update Consumer .....");
		long startTime = System.currentTimeMillis();
		IMessage proto = (IMessage) protocolType.getMesgSerializer(WioKafkaConstants.CONFIG_UPDATE_PROTO);
		logger.info(Thread.currentThread().getName() +"| Recieved "+proto.schema() + " message.");
		ConfigUpdates msgUpdates = (ConfigUpdates) proto.deSerialize(record.value());
		logger.info("Config Update msgUpdates : "+msgUpdates);
		process(msgUpdates);
		long endTime = System.currentTimeMillis();
		long timeTaken = endTime - startTime;
		logger.info("Time Taken by ConfigUpdateConsumer to process the ConfigUpdate is  : "+timeTaken);
	}

	public void process(ConfigUpdates lconfigUpds) {
		logger.info(Thread.currentThread().getName() +" Recieved message for the device Id = "+lconfigUpds.getDeviceid() + " Status = "+lconfigUpds.getStatus());

		if(lconfigUpds.getAction().equals("ConfigResponse"))
		{

			switch(lconfigUpds.getStatus().toUpperCase()) {

			case "SUCCESS":
				logger.info(Thread.currentThread().getName() +" Configuration applied succesfully on device = "+lconfigUpds.getDeviceid());

				ConfigSuccessRespHandler confSuccessHandler = new ConfigSuccessRespHandler();
				confSuccessHandler.processSuccessResp(lconfigUpds);
				break;
			case "FAILURE":

				logger.info(Thread.currentThread().getName() +" Configuration failed to apply on device = "+lconfigUpds.getDeviceid());
				logger.info(Thread.currentThread().getName() +" Failed to apply parameter(s) = "+configParam);

				ConfigFailureRespHandler confFailureHandler = new ConfigFailureRespHandler();
				confFailureHandler.processFailureResp(lconfigUpds);
				break;
			}
		}
		else if(lconfigUpds.getAction().equals("FwDownload"))
		{
			switch(lconfigUpds.getStatus().toUpperCase()) {

			case "SUCCESS":
				logger.info(Thread.currentThread().getName() +" Firmware Download Status = "+lconfigUpds.getDeviceid());

				FirmwareDownloadSuccessRespHandler fmwrDwnldSuccessRespHandler = new FirmwareDownloadSuccessRespHandler();
				fmwrDwnldSuccessRespHandler.processSuccessResp(lconfigUpds);
				break;
			case "FAILURE":

				logger.info(Thread.currentThread().getName() +"  Firmware Download Status "+lconfigUpds.getDeviceid());

				FirmwareDownloadFailureRespHandler fmwrDwnldFailureRespHandler = new FirmwareDownloadFailureRespHandler();
				fmwrDwnldFailureRespHandler.processFailureResp(lconfigUpds);
				break;
			}


		}
		else if(lconfigUpds.getAction().equals("FirmwareUpgrade"))
		{

			switch(lconfigUpds.getStatus().toUpperCase()) {

			case "SUCCESS":
				logger.info(Thread.currentThread().getName() +" Firmware Upgrade Status = "+lconfigUpds.getDeviceid());

				FirmwareUpgradeSuccessRespHandler fmwrUprdSuccessRespHandler = new FirmwareUpgradeSuccessRespHandler();
				fmwrUprdSuccessRespHandler.processSuccessResp(lconfigUpds);
				break;
			case "FAILURE":

				logger.info(Thread.currentThread().getName() +"  Firmware Upgrade Status "+lconfigUpds.getDeviceid());

				FirmwareUpgradeFailureRespHandler fmwrUprdFailureRespHandler = new FirmwareUpgradeFailureRespHandler();
				fmwrUprdFailureRespHandler.processFailureResp(lconfigUpds);
				break;
			}



		}


	}

}
