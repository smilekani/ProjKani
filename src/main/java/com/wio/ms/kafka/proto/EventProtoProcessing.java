/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jun-2018  16:32:56 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.proto.EventProtoProcessing.java
 *
 *
 */
package com.wio.ms.kafka.proto;

/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jun-2018  16:32:56 PM
 *
 */
import java.io.IOException;

import org.apache.log4j.Logger;

import com.wio.common.aws.api.es.document.JestIndexDocument;
import com.wio.common.protobuf.generated.WIOEventProto.WIOEVENT;
import com.wio.ms.kafka.utils.EventESUtil;
import com.wio.ms.kafka.utils.KafkaConfUtil;

import io.searchbox.core.DocumentResult;

public class EventProtoProcessing {

	private static final Logger log = Logger.getLogger(EventProtoProcessing.class);

	private static EventProtoProcessing eventProtoProcessing;

	private EventProtoProcessing() {
	}

	public static EventProtoProcessing getInstance()
	{
		if(eventProtoProcessing == null)
		{
			synchronized (EventProtoProcessing.class) 
			{
				eventProtoProcessing = new EventProtoProcessing();
			}
		}
		return eventProtoProcessing;
	}

	public void process(WIOEVENT mseg) {

		try {

			JestIndexDocument document = null;

			if(mseg.hasParp()) { 
				document = new JestIndexDocument(EventESUtil.jsonForEs(mseg.getParp()), KafkaConfUtil.EVENT_INDEX, KafkaConfUtil.EVENT_TYPE);
			}

			if(mseg.hasPdhcp()) { 
				document = new JestIndexDocument(EventESUtil.jsonForEs(mseg.getPdhcp()), KafkaConfUtil.EVENT_INDEX, KafkaConfUtil.EVENT_TYPE);
			}

			if(mseg.hasPap()) { 
				document = new JestIndexDocument(EventESUtil.jsonForEs(mseg.getPap()), KafkaConfUtil.EVENT_INDEX, KafkaConfUtil.EVENT_TYPE);
			}
			if(mseg.hasPwan()) { 
				document = new JestIndexDocument(EventESUtil.jsonForEs(mseg.getPwan()), KafkaConfUtil.EVENT_WAN_INDEX, KafkaConfUtil.EVENT_WAN_TYPE);
			}
			if(mseg.hasPmem()) { 
				document = new JestIndexDocument(EventESUtil.jsonForEs(mseg.getPmem()), KafkaConfUtil.EVENT_MEM_INDEX, KafkaConfUtil.EVENT_MEM_TYPE);
			}

			DocumentResult result = document.indexDocument();
			log.info(result.getJsonString());
		} catch (IOException e) {
			log.info(Thread.currentThread().getName() +"| "+e.toString());
			log.info(Thread.currentThread().getName() +"| "+e.getStackTrace());
			log.error("Exception Occurred while Pushing the ES Event... ", e);
			e.printStackTrace();
		}



	}

}
