/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.RadioProtoBufProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.generate.protobuf;

import java.text.SimpleDateFormat;
/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 */
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.framework.MessageProtocolFactory;
import com.wio.common.protobuf.impl.WIORadioProtoDataNew;
import com.wio.common.protobuf.model.WIORFAssoListMsgMO;
import com.wio.common.protobuf.model.WIORFAssoResMsgMO;
import com.wio.common.protobuf.model.WIORFAssoRxMO;
import com.wio.common.protobuf.model.WIORFAssoTxMO;
import com.wio.common.protobuf.model.WIORFEncryptionMO;
import com.wio.common.protobuf.model.WIORFMgntMsgMO;
import com.wio.common.protobuf.model.WIORFRadioInfoMsgMO;
import com.wio.common.protobuf.model.WIORFRadioInfoMsgMO.WIORFHardwareLocal;
import com.wio.common.protobuf.model.WIORFScanMsgMO;
import com.wio.common.protobuf.model.WIORFScanResultMO;
import com.wio.common.protobuf.model.WIORFSurveyMsgMO;
import com.wio.common.protobuf.model.WIORFSurveyResultMO;
import com.wio.common.protobuf.model.WIORadioProtoMO;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class RadioProtoBufProducer {

	private static final Logger logger = Logger.getLogger(RadioProtoBufProducer.class);

	public WIORadioProtoDataNew constructProtoBufMsg(int dataOrder) 
	{
		IMessageSerializer protocolType = MessageProtocolFactory.getFactoryInst().getMessageProtocol(WioKafkaConstants.PROTPBUF);
		WIORadioProtoDataNew proto = (WIORadioProtoDataNew) protocolType.getMesgSerializer(WioKafkaConstants.RADIO_METRIC);
		proto.messageConstructor();
		/**
		 *  One of the Case from Proto Msg - wio_radio.proto
		 */
		proto.serializeOneOf(1);

		WIORadioProtoMO wioRadioProtoMO = new WIORadioProtoMO();
		wioRadioProtoMO.setInterf("interf"+dataOrder);
		wioRadioProtoMO.setAction("action");
		SimpleDateFormat apTimeFormat = new SimpleDateFormat("yyyy:MM:dd'T'HH:mm:ss.SSS'Z'");
		Date date = new Date();
		String formattedDate = apTimeFormat.format(date);
		wioRadioProtoMO.setTime(formattedDate);
		wioRadioProtoMO.setDeviceid("deviceid");
		wioRadioProtoMO.setDftopic("dftopic");
		proto.setConfigUpdateDetails(wioRadioProtoMO);

		//Scan Msg........................

		WIORFScanMsgMO wioRfScanMsgProtoMO = constructScanObject();
		proto.setConfigUpdateDetails(wioRfScanMsgProtoMO);

		//Survey Msg........................
		WIORFSurveyMsgMO wiorfSurveyMsgMO = constructSurveyObject();
		proto.setConfigUpdateDetails(wiorfSurveyMsgMO);

		// Info Msg.......................
		WIORFRadioInfoMsgMO infoMsgMO = constructInfoObject();
		proto.setConfigUpdateDetails(infoMsgMO);

		// Assoc List Msg.....................
		WIORFAssoListMsgMO assoListMsgMO = constructAssocObject();
		proto.setConfigUpdateDetails(assoListMsgMO);

		// Mgnt Msg..........................
		WIORFMgntMsgMO mgntMsgMO = constructMgntObject();
		proto.setConfigUpdateDetails(mgntMsgMO);
		logger.info("After radio serialize == "+proto);
		return proto;
	}

	private WIORFScanMsgMO constructScanObject() {
		WIORFScanMsgMO wioRfScanMsgProtoMO = new WIORFScanMsgMO();
		List<WIORFScanResultMO> scanResults = new ArrayList<>();
		WIORFScanResultMO scanResult1 = new WIORFScanResultMO();
		scanResult1.setSsid("ssid");
		scanResult1.setBssid("bssid");
		scanResult1.setMode("mode");
		scanResult1.setChannel(1);
		scanResult1.setSignal(11);
		scanResult1.setQuality(111);
		scanResult1.setQualityMax(5);
		WIORFEncryptionMO encryption1 = new WIORFEncryptionMO();
		encryption1.setEnabled(true);
		List<Integer> wpa1 = new ArrayList<>();
		wpa1.add(01);
		wpa1.add(02);
		encryption1.setWpa(wpa1);
		List<String> authentication1 = new ArrayList<>();
		authentication1.add("Auth");
		authentication1.add("Auth1");
		authentication1.add("Auth2");
		encryption1.setAuthentication(authentication1);
		List<String> ciphers1 = new ArrayList<>();
		ciphers1.add("Cipher");
		ciphers1.add("Cipher1");
		ciphers1.add("Cipher2");
		encryption1.setCiphers(ciphers1);
		scanResult1.setEncryption(encryption1);
		scanResults.add(scanResult1);

		WIORFScanResultMO scanResult2 = new WIORFScanResultMO();
		scanResult2.setSsid("ssidq");
		scanResult2.setBssid("bssidq");
		scanResult2.setMode("modeq");
		scanResult2.setChannel(3);
		scanResult2.setSignal(2);
		scanResult2.setQuality(4);
		scanResult2.setQualityMax(5);
		WIORFEncryptionMO encryption2 = new WIORFEncryptionMO();
		encryption2.setEnabled(true);
		List<Integer> wpa2 = new ArrayList<>();
		wpa2.add(31);
		wpa2.add(32);
		encryption2.setWpa(wpa2);
		List<String> authentication2 = new ArrayList<>();
		authentication2.add("Aut2");
		//		authentication2.add("Aut1");
		//		authentication2.add("Aut3");
		encryption2.setAuthentication(authentication2);
		List<String> ciphers2 = new ArrayList<>();
		ciphers2.add("Cip1");
		//		ciphers2.add("Cip");
		//		ciphers2.add("Cipher2");
		encryption2.setCiphers(ciphers2);
		scanResult2.setEncryption(encryption2);
		scanResults.add(scanResult2);

		wioRfScanMsgProtoMO.setScanResults(scanResults);
		return wioRfScanMsgProtoMO;
	}

	private WIORFSurveyMsgMO constructSurveyObject() {
		WIORFSurveyMsgMO wiorfSurveyMsgMO = new WIORFSurveyMsgMO();
		List<WIORFSurveyResultMO> wiorfSurveyResultMOList = new ArrayList<>();
		WIORFSurveyResultMO wiorfSurveyResultMO = new WIORFSurveyResultMO();
		wiorfSurveyResultMO.setMhz(10);
		wiorfSurveyResultMO.setNoise(11);
		wiorfSurveyResultMO.setActiveTime(12);
		wiorfSurveyResultMO.setBusyTime(13);
		wiorfSurveyResultMO.setBusyTimeExt(14);
		wiorfSurveyResultMO.setRxTime(15);
		wiorfSurveyResultMO.setTxTime(16);
		wiorfSurveyResultMOList.add(wiorfSurveyResultMO);

		wiorfSurveyMsgMO.setSurveyResults(wiorfSurveyResultMOList);
		return wiorfSurveyMsgMO;
	}

	private WIORFRadioInfoMsgMO constructInfoObject() {
		WIORFRadioInfoMsgMO infoMsgMO = new WIORFRadioInfoMsgMO();
		infoMsgMO.setPhy("phy");
		infoMsgMO.setSsid("ssid");
		infoMsgMO.setBssid("bssid");
		infoMsgMO.setCountry("country");
		infoMsgMO.setMode("mode-1");
		infoMsgMO.setChannel(11);
		infoMsgMO.setFrequency(11);
		infoMsgMO.setFrequencyOffset(12);
		infoMsgMO.setTxpower(13);
		infoMsgMO.setTxpowerOffset(14);
		infoMsgMO.setQuality(15);
		infoMsgMO.setQualityMax(16);
		infoMsgMO.setNoise(17);
		infoMsgMO.setSignal(18);

		infoMsgMO.setBitrate(19);

		WIORFEncryptionMO encryption3 = new WIORFEncryptionMO();
		encryption3.setEnabled(true);
		List<Integer> wpa3 = new ArrayList<>();
		wpa3.add(31);
		wpa3.add(32);
		encryption3.setWpa(wpa3);
		List<String> authentication3 = new ArrayList<>();
		authentication3.add("Aut2");
		authentication3.add("Aut1");
		authentication3.add("Aut3");
		encryption3.setAuthentication(authentication3);
		List<String> ciphers3 = new ArrayList<>();
		ciphers3.add("Ci1");
		ciphers3.add("Ci2");
		ciphers3.add("Ci3");
		encryption3.setCiphers(ciphers3);
		infoMsgMO.setEncryption(encryption3);

		List<String> htmodes = new ArrayList<>();
		htmodes.add("htmodes");
		infoMsgMO.setHtmodes(htmodes);
		List<String> hwmodes = new ArrayList<>();
		hwmodes.add("hwmodes1");
		infoMsgMO.setHwmodes(hwmodes);

		WIORFHardwareLocal hardware = new WIORFHardwareLocal();
		List<Integer> ids = new ArrayList<>();
		ids.add(1);
		ids.add(2);
		hardware.setId(ids);
		hardware.setName("name");
		infoMsgMO.setHardware(hardware);
		return infoMsgMO;
	}

	private WIORFAssoListMsgMO constructAssocObject() {
		WIORFAssoListMsgMO assoListMsgMO = new WIORFAssoListMsgMO();
		List<WIORFAssoResMsgMO> assoResults = new ArrayList<>();
		WIORFAssoResMsgMO assoResult = new WIORFAssoResMsgMO();
		assoResult.setMac("mac");
		assoResult.setSignal(1);
		assoResult.setSignalAvg(2);
		assoResult.setNoise(3);
		assoResult.setInactive(4);
		assoResult.setConnectedTime(5);
		assoResult.setThr(6);
		assoResult.setAuthorized(false);
		assoResult.setAuthenticated(false);
		assoResult.setPreamble("preamble");
		assoResult.setWme(false);
		assoResult.setMfp(false);
		assoResult.setTdls(false);
		WIORFAssoRxMO rx = new WIORFAssoRxMO();
		rx.setDropMisc(11);
		rx.setPackets(12);
		rx.setBytes(13);
		rx.setRate(14);
		rx.setMcs(15);
		rx.setFortymhz(false);
		rx.setShortGi(false);
		assoResult.setRx(rx);
		WIORFAssoTxMO tx = new WIORFAssoTxMO();
		tx.setFailed(21);
		tx.setRetries(22);
		tx.setPackets(23);
		tx.setBytes(24);
		tx.setRate(25);
		tx.setMcs(26);
		tx.setFortymhz(false);
		tx.setShortGi(false);
		assoResult.setTx(tx);
		assoResults.add(assoResult);
		assoListMsgMO.setAssoResults(assoResults);
		return assoListMsgMO;
	}

	private WIORFMgntMsgMO constructMgntObject() {
		WIORFMgntMsgMO mgntMsgMO = new WIORFMgntMsgMO();
		mgntMsgMO.setFrameControl("frameControl");
		mgntMsgMO.setDuration("duration");
		mgntMsgMO.setDa("da");
		mgntMsgMO.setSa("sa");
		mgntMsgMO.setBssid("bssid");
		mgntMsgMO.setType("type");
		mgntMsgMO.setSeqCtrl("seqCtrl");

		WIORFEncryptionMO encryption4 = new WIORFEncryptionMO();
		encryption4.setEnabled(true);
		List<Integer> wpa4 = new ArrayList<>();
		wpa4.add(41);
		wpa4.add(42);
		encryption4.setWpa(wpa4);
		List<String> authentication4 = new ArrayList<>();
		authentication4.add("Aut1");
		authentication4.add("Aut2");
		authentication4.add("Aut3");
		encryption4.setAuthentication(authentication4);
		List<String> ciphers4 = new ArrayList<>();
		ciphers4.add("Cip1");
		ciphers4.add("Cip2");
		ciphers4.add("Cip3");
		encryption4.setCiphers(ciphers4);
		mgntMsgMO.setEncryption(encryption4);
		mgntMsgMO.setSignal(11);
		mgntMsgMO.setChannel(12);
		mgntMsgMO.setFrequency(13);
		mgntMsgMO.setFrequencyOffset(14);
		mgntMsgMO.setTxpower(15);
		return mgntMsgMO;
	}

}