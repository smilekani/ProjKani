/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.SpeedTestProtoBufProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.generate.protobuf;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.wio.common.protobuf.model.WioSpeedTest;
import com.wio.ms.kafka.consumer.model.SpeedTestObject;
import com.wio.ms.kafka.utils.KafkaConfUtil;

public class SpeedTestProtoBufProducer {

	private static final Logger logger = Logger.getLogger(SpeedTestProtoBufProducer.class);
	public static Map<String, WioSpeedTest> speedTestESData = new ConcurrentHashMap<>();

	public SpeedTestObject functn(String input)
	{
		Gson gson=new Gson();
		SpeedTestObject mesg=gson.fromJson(input, SpeedTestObject.class);
		WioSpeedTest obj = new WioSpeedTest();
		String speedType = null;
		Date time = null;

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
				}
				if (obj.getDownloadSpeed() != null && obj.getUploadSpeed() != null && obj.getPing() != null) {
					obj.setDeviceID(mesg.getDeviceid());
					try {
						time = KafkaConfUtil.apTimeFormat.parse(mesg.getLocal_time());
					} catch (ParseException e) {
						logger.error("ParseException = " + e);
					}
					obj.setTime(KafkaConfUtil.esTimeFormat.format(time));
					obj.setServerID(mesg.getServer_id());
					obj.setServerLocation(mesg.getServer_location());
					logger.info(gson.toJson(speedTestESData).toString());
					speedTestESData.remove(mesg.getDeviceid());
					logger.info(gson.toJson(speedTestESData).toString());
				}

				speedTestESData.put(mesg.getDeviceid(), obj);
			}
		}
		return mesg;
	}

	public String constructProtoBufMsg() throws IOException 
	{
		String atr = "{\"deviceid\":\"06aa74-f801-a401-b6ytgheb71171\",\"event\":\"finish\",\"local_time\":\"Tue May  8 11:15:23 2018\",\"speed_type\":\"ping\",\"value\":\"238.3\"}";
		SpeedTestObject functn = functn(atr);
		return functn.toString();		
	}

}
