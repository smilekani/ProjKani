/**
 * @author Kanimozhi.M
 *
 * @Dated 22-Jun-2018  16:02:06 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.proto.RadioProtoProcessing.java
 *
 *
 */
package com.wio.ms.kafka.proto;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.wio.common.aws.api.es.document.JestIndexDocument;
import com.wio.common.protobuf.generated.WIORadioProtoNew.WIORADIO;
import com.wio.ms.kafka.utils.KafkaConfUtil;
import com.wio.ms.kafka.utils.RadioESUtil;

import io.searchbox.core.DocumentResult;

public class RadioProtoProcessing 
{
	private static final Logger log = Logger.getLogger(RadioProtoProcessing.class);

	private static RadioProtoProcessing radioProtoProcessing;

	private RadioProtoProcessing() 
	{
	}

	public static RadioProtoProcessing getInstance()
	{
		if(radioProtoProcessing == null)
		{
			synchronized (RadioProtoProcessing.class) 
			{
				radioProtoProcessing = new RadioProtoProcessing();
			}
		}
		return radioProtoProcessing;
	}

	public void process(WIORADIO wioRadio) {

		try {

			JestIndexDocument document = null;

			if(wioRadio.hasScanMsg()) { 
				document = new JestIndexDocument(RadioESUtil.jsonForScanMsg(wioRadio.getScanMsg(), wioRadio.getInterface(),wioRadio.getAction(),
						wioRadio.getTime(), wioRadio.getDeviceid(), wioRadio.getDftopic()), KafkaConfUtil.RADIO_INDEX, KafkaConfUtil.RADIO_TYPE);
			}

			if(wioRadio.hasSurveyMsg()) { 
				document = new JestIndexDocument(RadioESUtil.jsonForSurveyMsg(wioRadio.getSurveyMsg(), wioRadio.getInterface(), wioRadio.getAction(),
						wioRadio.getTime(), wioRadio.getDeviceid(), wioRadio.getDftopic()), KafkaConfUtil.RADIO_INDEX, KafkaConfUtil.RADIO_TYPE);
			}

			if(wioRadio.hasInfoMsg()) { 
				document = new JestIndexDocument(RadioESUtil.jsonForInfoMsg(wioRadio.getInfoMsg(), wioRadio.getInterface(), wioRadio.getAction(),
						wioRadio.getTime(), wioRadio.getDeviceid(), wioRadio.getDftopic()), KafkaConfUtil.RADIO_INDEX, KafkaConfUtil.RADIO_TYPE);
			}

			if(wioRadio.hasAssocListMsg()) {
				document = new JestIndexDocument(RadioESUtil.jsonForAssocListMsg(wioRadio.getAssocListMsg(), wioRadio.getInterface(), wioRadio.getAction(),
						wioRadio.getTime(), wioRadio.getDeviceid(), wioRadio.getDftopic()), KafkaConfUtil.RADIO_INDEX, KafkaConfUtil.RADIO_TYPE);
			}

			if(wioRadio.hasMgmtMsg()) {
				document = new JestIndexDocument(RadioESUtil.jsonForMgntMsg(wioRadio.getMgmtMsg(), wioRadio.getInterface(), wioRadio.getAction(),
						wioRadio.getTime(), wioRadio.getDeviceid(), wioRadio.getDftopic()), KafkaConfUtil.RADIO_INDEX, KafkaConfUtil.RADIO_TYPE);
			}

			DocumentResult result = document.indexDocument();
			log.info(result.getJsonString());
		} catch (IOException e) {
			log.info(Thread.currentThread().getName() +"| "+e.toString());
			log.info(Thread.currentThread().getName() +"| "+e.getStackTrace());
			log.error("Exception Occurred while Pushing the Radio for ES... ", e);
			e.printStackTrace();
		}


	}
}
