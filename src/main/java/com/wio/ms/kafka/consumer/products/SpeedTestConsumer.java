/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  14:55:53 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.SpeedTestConsumer.java
 *
 *
 */
package com.wio.ms.kafka.consumer.products;

/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  14:55:53 PM
 *
 */
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.wio.common.aws.api.es.document.JestIndexDocument;
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.model.WioSpeedTest;
import com.wio.ms.kafka.consumer.factory.WioConsumer;
import com.wio.ms.kafka.consumer.model.SpeedTestObject;
import com.wio.ms.kafka.utils.KafkaConfUtil;

import io.searchbox.core.DocumentResult;

public class SpeedTestConsumer extends WioConsumer {

	private static final Logger logger = Logger.getLogger(SpeedTestConsumer.class);

	static Gson gson=new Gson();
	public static Map<String, WioSpeedTest> speedTestESData = new ConcurrentHashMap<>();

	public SpeedTestConsumer(String groupId, String topic, KafkaConsumer<byte[], byte[]> consumer) {
		super(groupId, topic, consumer);
	}

	@Override
	public void processJsonRecords(ConsumerRecord<byte[], byte[]> record) {
		logger.info("processing Records in SpeedTest....");
		long startTime = System.currentTimeMillis();
		SpeedTestObject mesg = null;
		try {
			mesg = gson.fromJson(new String(record.value()), SpeedTestObject.class);
			logger.info("SpeedTest mesg : "+mesg);
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
		}
		esProcess(mesg);
		long endTime = System.currentTimeMillis();
		long timeTaken = endTime - startTime;
		logger.info("Time Taken by SpeedTestConsumer to process the SpeedTest is  : "+timeTaken);
	}

	public void esProcess(SpeedTestObject mesg)
	{
		logger.info(Thread.currentThread().getName()+" ========== Starting ESOprocess ==========");
		WioSpeedTest obj = new WioSpeedTest();
		String speedType = null;
		if (mesg.getEvent().equalsIgnoreCase("finish")) {
			speedTestESData.putIfAbsent(mesg.getDeviceid(), obj);
			if (speedTestESData.containsKey(mesg.getDeviceid())) {
				obj = speedTestESData.get(mesg.getDeviceid());
				speedType = mesg.getSpeed_type();
				switch (speedType) {
				case "download":
					obj.setDownloadSpeed(mesg.getSpeed());
					break;
				case "upload":
					obj.setUploadSpeed(mesg.getSpeed());
					break;
				case "ping":
					obj.setPing(mesg.getValue());
					break;
				default:
					break;
				}
				if (obj.getDownloadSpeed() != null && obj.getUploadSpeed() != null && obj.getPing() != null) {
					obj.setDeviceID(mesg.getDeviceid());
					obj.setTime(KafkaConfUtil.timestampToUtc(mesg.getLocal_time()));
					obj.setServerID(mesg.getServer_id());
					obj.setServerLocation(mesg.getServer_location());
					process(obj);
					speedTestESData.remove(mesg.getDeviceid());
				}else {
					speedTestESData.put(mesg.getDeviceid(), obj);
				}

			}
		}
	}

	public void process(WioSpeedTest mesg) {

		try {
			//			WioSpeedTest wioSpeedTest = EsUtil.jsonForEs(mseg);
			logger.info(Thread.currentThread().getName()+ "| "+mesg.toString());
			JestIndexDocument document = new JestIndexDocument(mesg, KafkaConfUtil.SPEEDTEST_INDEX, KafkaConfUtil.SPEEDTEST_TYPE);
			DocumentResult result = document.indexDocument();
			logger.info(Thread.currentThread().getName()+ "| ES output ==>> "+result.getJsonString());
		} catch (IOException e) {
			logger.error(Thread.currentThread().getName() + "| " + e);
		}

	}

	@Override
	public void processRecords(IMessageSerializer protocolType, ConsumerRecord<byte[], byte[]> record) {

	}
}
