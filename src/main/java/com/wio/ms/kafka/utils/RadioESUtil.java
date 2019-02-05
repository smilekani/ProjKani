/**
 * @author Kanimozhi.M
 *
 * @Dated 27-Jul-2018  4:25:25 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.utils.RadioESUtil.java
 *
 *
 */
package com.wio.ms.kafka.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.google.protobuf.ProtocolStringList;
import com.wio.common.aws.api.es.event.model.WIORFAssocListMsgESMO;
import com.wio.common.aws.api.es.event.model.WIORFAssocResultESMO;
import com.wio.common.aws.api.es.event.model.WIORFAssocResultESMO.WIORFAssocResultRXESMO;
import com.wio.common.aws.api.es.event.model.WIORFAssocResultESMO.WIORFAssocResultTXESMO;
import com.wio.common.aws.api.es.event.model.WIORFEncryptionESMO;
import com.wio.common.aws.api.es.event.model.WIORFInfoMsgESMO;
import com.wio.common.aws.api.es.event.model.WIORFInfoMsgESMO.WIORFInfoMsgHardwareESMO;
import com.wio.common.aws.api.es.event.model.WIORFMgntMsgESMO;
import com.wio.common.aws.api.es.event.model.WIORFScanMsgESMO;
import com.wio.common.aws.api.es.event.model.WIORFScanResultESMO;
import com.wio.common.aws.api.es.event.model.WIORFSurveyMsgESMO;
import com.wio.common.aws.api.es.event.model.WIORFSurveyResultESMO;
import com.wio.common.protobuf.generated.WIORadioProtoNew.ENCRYPTION;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_ASSOC_LIST_MSG;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_ASSOC_LIST_MSG.ASSOC_RESULTS;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_ASSOC_LIST_MSG.ASSOC_RESULTS.RX;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_ASSOC_LIST_MSG.ASSOC_RESULTS.TX;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_INFO_MSG;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_INFO_MSG.HARDWARE;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_MANAGEMENT;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_SCAN_MSG;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_SCAN_MSG.SCAN_RESULTS;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_SURVEY_MSG;
import com.wio.common.protobuf.generated.WIORadioProtoNew.RADIO_SURVEY_MSG.SURVEY_RESULTS;
import com.wio.common.validation.RequestValidator;

/**
 * @author Kanimozhi.M
 *
 * @Dated 27-Jul-2018  4:25:25 PM
 *
 */
public class RadioESUtil {

	static TimeZone tz = TimeZone.getTimeZone("UTC");
	static SimpleDateFormat apTimeFormat = new SimpleDateFormat("yyyy:MM:dd'T'HH:mm:ss.SSS'Z'");
	static SimpleDateFormat esTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	static String radioInterface = "wlan0";
	static String eRadioType1 = "2.4 GHz";
	static String eRadioType2 = "5.0 GHz";

	//  Example Date Field
	//	2018:07:30T10:17:58.149736Z

	private static final Logger log = Logger.getLogger(RadioESUtil.class);

	public static WIORFScanMsgESMO jsonForScanMsg(RADIO_SCAN_MSG rfScanMsg, String interf, String action, 
			String scanMsgtime, String deviceId, String dfTopic) {

		Date time = null;

		try {
			log.info("scanMsgtime : "+scanMsgtime);
			time = apTimeFormat.parse(scanMsgtime);
			log.info("RADIO_SCAN_MSG time ============> "+time);
			esTimeFormat.setTimeZone(tz);
			apTimeFormat.setTimeZone(tz);
		} catch (ParseException e) {
			log.error(e.fillInStackTrace());
		} catch (NumberFormatException ne) {
			log.error("NumberFormatException: "+ne.fillInStackTrace());
		}catch (Exception ne) {
			log.error("Exception: "+ne.fillInStackTrace());
		}
		WIORFScanMsgESMO scanMsgESMO = new WIORFScanMsgESMO();
		scanMsgESMO.setInterf(interf);
		if(interf.equals(radioInterface))
			scanMsgESMO.setEradio_type(eRadioType1);
		else
			scanMsgESMO.setEradio_type(eRadioType2);
		scanMsgESMO.setAction(action);
		scanMsgESMO.setTime(esTimeFormat.format(time));
		scanMsgESMO.setDeviceid(deviceId);
		scanMsgESMO.setDevicegroup(dfTopic);
		List<WIORFScanResultESMO> scanResult = new ArrayList<>();
		List<SCAN_RESULTS> scanResultsList = rfScanMsg.getScanResultsList();
		if(!RequestValidator.isEmptyCollection(scanResultsList))
		{
			for (SCAN_RESULTS eachScanResult : scanResultsList) {
				WIORFScanResultESMO scanResultESMO = new WIORFScanResultESMO();
				scanResultESMO.setSsid(eachScanResult.getSsid());
				scanResultESMO.setBssid(eachScanResult.getBssid());
				scanResultESMO.setMode(eachScanResult.getMode());
				scanResultESMO.setChannel(eachScanResult.getChannel());
				scanResultESMO.setSignal(eachScanResult.getSignal());
				scanResultESMO.setQuality(eachScanResult.getQuality());
				scanResultESMO.setQuality_max(eachScanResult.getQualityMax());
				WIORFEncryptionESMO encryption = new WIORFEncryptionESMO();
				ENCRYPTION encryptionObj = eachScanResult.getEncryption();
				encryption.setEnabled(encryptionObj.getEnabled());
				List<Integer> wpaList = encryptionObj.getWpaList();
				if(!RequestValidator.isEmptyCollection(wpaList))
				{
					List<Integer> wpas = new ArrayList<>();
					for (Integer wpa : wpaList) {
						wpas.add(wpa);
					}
					encryption.setWpa(wpas);
				}
				ProtocolStringList authenticationList = encryptionObj.getAuthenticationList();
				if(!RequestValidator.isEmptyCollection(authenticationList))
				{
					List<String> authentications = new ArrayList<>();
					for (String authentication : authenticationList) {
						authentications.add(authentication);
					}
					encryption.setAuthentication(authentications);
				}
				ProtocolStringList ciphersList = encryptionObj.getCiphersList();
				if(!RequestValidator.isEmptyCollection(ciphersList))
				{
					List<String> ciphers = new ArrayList<>();
					for (String cipher : ciphers) {
						ciphers.add(cipher);
					}
					encryption.setCiphers(ciphers);
				}
				scanResultESMO.setEncryption(encryption);
				scanResult.add(scanResultESMO);
			}
			scanMsgESMO.setScanResult(scanResult);
		}
		return scanMsgESMO;
	}

	public static WIORFSurveyMsgESMO jsonForSurveyMsg(RADIO_SURVEY_MSG rfSurveyMsg, String interf, String action, 
			String surveyMsgtime, String deviceId, String dfTopic) {

		Date time = null;

		try {
			log.info("surveyMsgtime : "+surveyMsgtime);
			time = apTimeFormat.parse(surveyMsgtime);
			esTimeFormat.setTimeZone(tz);
			apTimeFormat.setTimeZone(tz);
			log.info("RADIO_SURVEY_MSG time ============> "+time);
		} catch (ParseException e) {
			log.error(e.fillInStackTrace());
		} catch (NumberFormatException ne) {
			log.error("NumberFormatException: "+ne.fillInStackTrace());
		}catch (Exception ne) {
			log.error("Exception: "+ne.fillInStackTrace());
		}
		WIORFSurveyMsgESMO surveyMsgESMO = new WIORFSurveyMsgESMO();
		surveyMsgESMO.setInterf(interf);
		if(interf.equals(radioInterface))
			surveyMsgESMO.setEradio_type(eRadioType1);
		else
			surveyMsgESMO.setEradio_type(eRadioType2);
		surveyMsgESMO.setAction(action);
		surveyMsgESMO.setTime(esTimeFormat.format(time));
		surveyMsgESMO.setDeviceid(deviceId);
		surveyMsgESMO.setDevicegroup(dfTopic);
		List<WIORFSurveyResultESMO> surveyResult = new ArrayList<>();
		List<SURVEY_RESULTS> surveyResultsList = rfSurveyMsg.getSurveyResultsList();
		if(!RequestValidator.isEmptyCollection(surveyResultsList))
		{
			for (SURVEY_RESULTS eachSurveyResult : surveyResultsList) {
				WIORFSurveyResultESMO surveyResultESMO = new WIORFSurveyResultESMO();
				surveyResultESMO.setMhz(eachSurveyResult.getMhz());
				surveyResultESMO.setNoise(eachSurveyResult.getNoise());
				surveyResultESMO.setActive_time(eachSurveyResult.getActiveTime());
				surveyResultESMO.setBusy_time(eachSurveyResult.getBusyTime());
				surveyResultESMO.setBusy_time_ext(eachSurveyResult.getBusyTimeExt());
				surveyResultESMO.setRx_time(eachSurveyResult.getRxTime());
				surveyResultESMO.setTx_time(eachSurveyResult.getTxTime());
				surveyResult.add(surveyResultESMO);
			}
			surveyMsgESMO.setSurveyResult(surveyResult);
		}
		return surveyMsgESMO;
	}

	public static WIORFInfoMsgESMO jsonForInfoMsg(RADIO_INFO_MSG rfSurveyMsg, String interf, String action, 
			String infoMsgtime, String deviceId, String dfTopic) {

		Date time = null;

		try {
			log.info("infoMsgtime : "+infoMsgtime);
			time = apTimeFormat.parse(infoMsgtime);
			esTimeFormat.setTimeZone(tz);
			apTimeFormat.setTimeZone(tz);
			log.info("RADIO_INFO_MSG time ============> "+time);
		} catch (ParseException e) {
			log.error(e.fillInStackTrace());
		} catch (NumberFormatException ne) {
			log.error("NumberFormatException: "+ne.fillInStackTrace());
		}catch (Exception ne) {
			log.error("Exception: "+ne.fillInStackTrace());
		}
		WIORFInfoMsgESMO infoMsgESMO = new WIORFInfoMsgESMO();
		infoMsgESMO.setInterf(interf);
		if(interf.equals(radioInterface))
			infoMsgESMO.setEradio_type(eRadioType1);
		else
			infoMsgESMO.setEradio_type(eRadioType2);
		infoMsgESMO.setAction(action);
		infoMsgESMO.setTime(esTimeFormat.format(time));
		infoMsgESMO.setDeviceid(deviceId);
		infoMsgESMO.setDevicegroup(dfTopic);
		infoMsgESMO.setPhy(rfSurveyMsg.getPhy());
		infoMsgESMO.setSsid(rfSurveyMsg.getSsid());
		infoMsgESMO.setBssid(rfSurveyMsg.getBssid());
		infoMsgESMO.setCountry(rfSurveyMsg.getCountry());
		infoMsgESMO.setMode(rfSurveyMsg.getMode());
		infoMsgESMO.setChannel(rfSurveyMsg.getChannel());
		infoMsgESMO.setFrequency(rfSurveyMsg.getFrequency());
		infoMsgESMO.setFrequency_offset(rfSurveyMsg.getFrequencyOffset());
		infoMsgESMO.setTxpower(rfSurveyMsg.getTxpower());
		infoMsgESMO.setTxpower_offset(rfSurveyMsg.getTxpowerOffset());
		infoMsgESMO.setQuality(rfSurveyMsg.getQuality());
		infoMsgESMO.setQuality_max(rfSurveyMsg.getQualityMax());
		infoMsgESMO.setNoise(rfSurveyMsg.getNoise());
		infoMsgESMO.setSignal(rfSurveyMsg.getSignal());
		infoMsgESMO.setBitrate(rfSurveyMsg.getBitrate());
		WIORFEncryptionESMO encryption = new WIORFEncryptionESMO();
		ENCRYPTION encryptionObj = rfSurveyMsg.getEncryption();
		encryption.setEnabled(encryptionObj.getEnabled());
		List<Integer> wpas = new ArrayList<>();
		List<Integer> wpaList = encryptionObj.getWpaList();
		if(!RequestValidator.isEmptyCollection(wpaList))
		{
			for (Integer wpa : wpaList) {
				wpas.add(wpa);
			}
			encryption.setWpa(wpas);
		}
		List<String> authentications = new ArrayList<>();
		List<String> authenticationList = encryptionObj.getAuthenticationList();
		if(!RequestValidator.isEmptyCollection(authenticationList))
		{
			for (String eachAuthentication : authenticationList) {
				authentications.add(eachAuthentication);
			}
			encryption.setAuthentication(authentications);
		}
		List<String> ciphers = new ArrayList<>();
		List<String> cipherList = encryptionObj.getCiphersList();
		if(!RequestValidator.isEmptyCollection(cipherList))
		{
			for (String cipher : cipherList) {
				ciphers.add(cipher);
			}
			encryption.setCiphers(ciphers);
		}
		infoMsgESMO.setEncryption(encryption);

		List<String> htmodes = new ArrayList<>();
		List<String> hwmodes = new ArrayList<>();

		ProtocolStringList htmodesList = rfSurveyMsg.getHtmodesList();
		if(!RequestValidator.isEmptyCollection(htmodesList))
		{
			for (String htmode : htmodesList) {
				htmodes.add(htmode);
			}
		}

		ProtocolStringList hwmodesList = rfSurveyMsg.getHwmodesList();
		if(!RequestValidator.isEmptyCollection(hwmodesList))
		{
			for (String hwmode : hwmodesList) {
				hwmodes.add(hwmode);
			}
		}
		infoMsgESMO.setHtmodes(htmodes);
		infoMsgESMO.setHwmodes(hwmodes);

		WIORFInfoMsgHardwareESMO hardwareES = new WIORFInfoMsgHardwareESMO();
		List<Integer> ids = new ArrayList<>();
		HARDWARE hardwareObj = rfSurveyMsg.getHardware();
		List<Integer> idList = hardwareObj.getIdList();
		if(!RequestValidator.isEmptyCollection(idList))
		{
			for (Integer id : idList) {
				ids.add(id);
			}
			hardwareES.setId(ids);
		}
		hardwareES.setName(hardwareObj.getName());
		infoMsgESMO.setHardware(hardwareES);

		return infoMsgESMO;
	}

	public static WIORFAssocListMsgESMO jsonForAssocListMsg(RADIO_ASSOC_LIST_MSG rfAssocListMsg, String interf, String action, 
			String assocMsgtime, String deviceId, String dfTopic) {

		Date time = null;

		try {
			log.info("assocMsgtime : "+assocMsgtime);
			time = apTimeFormat.parse(assocMsgtime);
			esTimeFormat.setTimeZone(tz);
			apTimeFormat.setTimeZone(tz);
			log.info("RADIO_ASSOC_LIST_MSG time ============> "+time);
		} catch (ParseException e) {
			log.error(e.fillInStackTrace());
		} catch (NumberFormatException ne) {
			log.error("NumberFormatException: "+ne.fillInStackTrace());
		}catch (Exception ne) {
			log.error("Exception: "+ne.fillInStackTrace());
		}
		WIORFAssocListMsgESMO assocListMsgESMO = new WIORFAssocListMsgESMO();
		assocListMsgESMO.setInterf(interf);
		if(interf.equals(radioInterface))
			assocListMsgESMO.setEradio_type(eRadioType1);
		else
			assocListMsgESMO.setEradio_type(eRadioType2);
		assocListMsgESMO.setAction(action);
		assocListMsgESMO.setTime(esTimeFormat.format(time));
		assocListMsgESMO.setDeviceid(deviceId);
		assocListMsgESMO.setDevicegroup(dfTopic);
		List<WIORFAssocResultESMO> assocResult = new ArrayList<>();
		List<ASSOC_RESULTS> assocResultList = rfAssocListMsg.getAssocResultsList();
		if(!RequestValidator.isEmptyCollection(assocResultList))
		{
			for (ASSOC_RESULTS wiorfAssocResultESMO : assocResultList) {
				WIORFAssocResultESMO assocResultESMO = new WIORFAssocResultESMO();
				assocResultESMO.setCmac(wiorfAssocResultESMO.getMac());
				assocResultESMO.setSignal(wiorfAssocResultESMO.getSignal());
				assocResultESMO.setSignal_avg(wiorfAssocResultESMO.getSignalAvg());
				assocResultESMO.setNoise(wiorfAssocResultESMO.getNoise());
				assocResultESMO.setInactive(wiorfAssocResultESMO.getInactive());
				assocResultESMO.setConnected_time(wiorfAssocResultESMO.getConnectedTime());
				assocResultESMO.setThr(wiorfAssocResultESMO.getThr());
				assocResultESMO.setAuthorized(wiorfAssocResultESMO.getAuthorized());
				assocResultESMO.setAuthenticated(wiorfAssocResultESMO.getAuthenticated());
				assocResultESMO.setPreamble(wiorfAssocResultESMO.getPreamble());
				assocResultESMO.setWme(wiorfAssocResultESMO.getWme());
				assocResultESMO.setMfp(wiorfAssocResultESMO.getMfp());
				assocResultESMO.setTdls(wiorfAssocResultESMO.getTdls());
				WIORFAssocResultRXESMO resultRXESMO = new WIORFAssocResultRXESMO();
				RX rx = wiorfAssocResultESMO.getRx();
				resultRXESMO.setDrop_misc(rx.getDropMisc());
				resultRXESMO.setPackets(rx.getPackets());
				resultRXESMO.setBytes(rx.getBytes());
				resultRXESMO.setRate(rx.getRate());
				resultRXESMO.setMcs(rx.getMcs());
				resultRXESMO.setFortymhz(rx.getFortymhz());
				resultRXESMO.setShort_gi(rx.getShortGi());
				assocResultESMO.setRx(resultRXESMO);
				WIORFAssocResultTXESMO resultTXESMO = new WIORFAssocResultTXESMO();
				TX tx = wiorfAssocResultESMO.getTx();
				resultTXESMO.setFailed(tx.getFailed());
				resultTXESMO.setRetries(tx.getRetries());
				resultTXESMO.setPackets(tx.getPackets());
				resultTXESMO.setBytes(tx.getBytes());
				resultTXESMO.setRate(tx.getRate());
				resultTXESMO.setMcs(tx.getMcs());
				resultTXESMO.setFortymhz(tx.getFortymhz());
				resultTXESMO.setShort_gi(tx.getShortGi());
				assocResultESMO.setTx(resultTXESMO);
				assocResult.add(assocResultESMO);
			}
		}
		assocListMsgESMO.setAssocResult(assocResult);
		return assocListMsgESMO;
	}

	public static WIORFMgntMsgESMO jsonForMgntMsg(RADIO_MANAGEMENT rfMgntMsg, String interf, String action, 
			String mgntMsgtime, String deviceId, String dfTopic) {

		Date time = null;

		try {
			log.info("mgntMsgtime : "+mgntMsgtime);
			time = apTimeFormat.parse(mgntMsgtime);
			esTimeFormat.setTimeZone(tz);
			apTimeFormat.setTimeZone(tz);
			log.info("RADIO_MANAGEMENT time ============> "+time);
		} catch (ParseException e) {
			log.error(e.fillInStackTrace());
		} catch (NumberFormatException ne) {
			log.error("NumberFormatException: "+ne.fillInStackTrace());
		}catch (Exception ne) {
			log.error("Exception: "+ne.fillInStackTrace());
		}
		WIORFMgntMsgESMO mgntMsgESMO = new WIORFMgntMsgESMO();
		mgntMsgESMO.setInterf(interf);
		if(interf.equals(radioInterface))
			mgntMsgESMO.setEradio_type(eRadioType1);
		else
			mgntMsgESMO.setEradio_type(eRadioType2);
		mgntMsgESMO.setAction(action);
		mgntMsgESMO.setTime(esTimeFormat.format(time));
		mgntMsgESMO.setDeviceid(deviceId);
		mgntMsgESMO.setDevicegroup(dfTopic);
		mgntMsgESMO.setFrame_control(rfMgntMsg.getFrameControl());
		mgntMsgESMO.setDuration(rfMgntMsg.getDuration());
		mgntMsgESMO.setDa(rfMgntMsg.getDa());
		mgntMsgESMO.setSa(rfMgntMsg.getSa());
		mgntMsgESMO.setBssid(rfMgntMsg.getBssid());
		mgntMsgESMO.setType(rfMgntMsg.getType());
		mgntMsgESMO.setSeq_ctrl(rfMgntMsg.getSeqCtrl());
		return mgntMsgESMO;
	}


}
