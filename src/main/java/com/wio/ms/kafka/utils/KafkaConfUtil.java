/**
 * @author Kanimozhi.M
 *
 * @Dated 16-Jun-2018  16:32:56 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.utils.KafkaConfUtil.java
 *
 *
 */
package com.wio.ms.kafka.utils;

/**
 * @author Kanimozhi.M
 *
 * @Dated 16-Jun-2018  16:32:56 PM
 *
 */
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.Logger;

public class KafkaConfUtil 
{
	public static final String WIO_LOG_TOPIC = System.getenv("WIO_LOG_TOPIC");
	public static final String WIO_LOG_GROUP_ID = System.getenv("WIO_LOG_GROUP_ID");
	public static final String WIO_EVENT_TOPIC = System.getenv("WIO_EVENT_TOPIC");
	public static final String WIO_EVENT_GROUP_ID = System.getenv("WIO_EVENT_GROUP_ID");
	public static final String WIO_RADIO_TOPIC = System.getenv("WIO_RADIO_TOPIC");
	public static final String WIO_RADIO_GROUP_ID = System.getenv("WIO_RADIO_GROUP_ID");
	public static final String WIO_FINGER_PRINT_TOPIC = System.getenv("WIO_FINGER_PRINT_TOPIC");
	public static final String WIO_FINGER_PRINT_GROUP_ID = System.getenv("WIO_FINGER_PRINT_GROUP_ID");
	public static final String WIO_SPEEDTEST_TOPIC = System.getenv("WIO_SPEEDTEST_TOPIC");
	public static final String WIO_SPEEDTEST_GROUP_ID = System.getenv("WIO_SPEEDTEST_GROUP_ID");
	public static final String WIO_CONFIG_UPDATE_TOPIC = System.getenv("WIO_CONFIG_UPDATE_TOPIC");
	public static final String WIO_CONFIG_UPDATE_GROUP_ID = System.getenv("WIO_CONFIG_UPDATE_GROUP_ID");
	public static final String WIO_DNSREQUEST_TOPIC = System.getenv("WIO_DNSREQUEST_TOPIC");
	public static final String WIO_DNSREQUEST_GROUP_ID = System.getenv("WIO_DNSREQUEST_GROUP_ID");
	
	// ES Index and Type Configurations
	
	public static final String LOG_INDEX = System.getenv("ES_LOG_INDEX");
	public static final String LOG_TYPE = System.getenv("ES_LOG_TYPE");
	public static final String EVENT_INDEX = System.getenv("ES_EVENT_INDEX");
	public static final String EVENT_TYPE = System.getenv("ES_EVENT_TYPE");
	public static final String RADIO_INDEX = System.getenv("ES_RADIO_INDEX");
	public static final String RADIO_TYPE = System.getenv("ES_RADIO_TYPE");
	public static final String DNS_INDEX = System.getenv("ES_DNS_INDEX");
	public static final String DNS_TYPE = System.getenv("ES_DNS_TYPE");
	public static final String EVENT_WAN_INDEX = System.getenv("ES_EVENT_WAN_INDEX");
	public static final String EVENT_WAN_TYPE = System.getenv("ES_EVENT_WAN_TYPE");
	public static final String EVENT_MEM_INDEX = System.getenv("ES_EVENT_MEM_INDEX");
	public static final String EVENT_MEM_TYPE = System.getenv("ES_EVENT_MEM_TYPE");
	public static final String SPEEDTEST_INDEX = System.getenv("ES_SPEEDTEST_INDEX");
	public static final String SPEEDTEST_TYPE = System.getenv("ES_SPEEDTEST_TYPE");

	private static final Logger log = Logger.getLogger(KafkaConfUtil.class);
	
	public static TimeZone tz = TimeZone.getTimeZone("UTC");
	public static SimpleDateFormat apTimeFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
	public static SimpleDateFormat esTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	
	public static String timestampToUtc(String date)
	{
		String formattedTime = null;
		TimeZone tz = TimeZone.getTimeZone("UTC");
		Date time = null;
		esTimeFormat.setTimeZone(tz);
		apTimeFormat.setTimeZone(tz);
		try {
			time = apTimeFormat.parse(date);
			formattedTime = esTimeFormat.format(time);
		} catch (ParseException e) {
			log.error("ParseException:- "+e);
		}
		return formattedTime;
	}

}
