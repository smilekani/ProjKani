/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  16:15:50 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.db.ConfigSuccessRespHandler.java
 *
 *
 */
package com.wio.ms.kafka.consumers.db;

/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  16:15:50 PM
 *
 */
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.wio.common.Enums.Constants.ConfigUpdateStatus;
import com.wio.common.config.api.DeviceConfigurationAPI;
import com.wio.common.config.model.ConfiguredValuesMO;
import com.wio.common.config.model.Device;
import com.wio.common.config.model.DeviceRadioSettings;
import com.wio.common.config.model.LANSettings;
import com.wio.common.config.model.Network;
import com.wio.common.config.model.WANSettings;
import com.wio.common.config.model.WiFi;
import com.wio.common.config.request.DeviceConfiguration;
import com.wio.common.config.request.NetworkSettings;
import com.wio.common.config.request.WiFiSettings;
import com.wio.common.exception.DAOException;
import com.wio.common.generic.dao.DDBGenericDAO;
import com.wio.common.generic.dao.FilterCondition;
import com.wio.common.generic.dao.QueryHelper;
import com.wio.common.generic.dao.helper.DynamoDBHelper;
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.framework.MessageProtocolFactory;
import com.wio.common.protobuf.generated.ConfigUpdatesProto.ConfigUpdates;
import com.wio.common.protobuf.impl.ConfigProtoData;
import com.wio.common.validation.RequestValidator;

/**
 * 
 * 1. get runningConfig NW setting and updatedConfig NW setting 
 * 2. compare the runningConfig NWID = updatedConfig NWID and 
 * 		if matches get the updatedConfig values and set in the corresponding setter parameter in the runningConfig.
 * 3. if doesn't match with any NWID in the runningConfig then, skip 
 * that updatedConfig which is not for this device (Very rare scenario).  
 * 
 * @author mageshb
 *
 */
public class ConfigSuccessRespHandler {
	
	private String deviceID ="";
	
	private Device device = null;
	private List<DeviceRadioSettings> radioSettingsList;
	private List<LANSettings> lanSettingsList;
	private WANSettings runningWanSetting;	
	private List<WiFiSettings> wifiSettingsList;
	
	private DeviceConfiguration runningConfig = null;
	private DeviceConfiguration updatedConfig = null;
	
	private ConfiguredValuesMO configUpdates = null;
	
	private DDBGenericDAO genDao = DDBGenericDAO.getInstance();
	
	private static final Logger logger = Logger.getLogger(ConfigSuccessRespHandler.class);
	
	public Boolean processSuccessResp(ConfigUpdates configUpdateMsg) {
		IMessageSerializer protocolType = MessageProtocolFactory.getFactoryInst().getMessageProtocol("PROTPBUF");
		ConfigProtoData proto = (ConfigProtoData) protocolType.getMesgSerializer("CONFIG_UPDATE");
		proto.messageConstructor();
		
		deviceID = configUpdateMsg.getDeviceid();
		try {
			device = genDao.getGenericObject(Device.class, deviceID);
		} catch (DAOException e) {
			e.printStackTrace();
		}
		
		getUpdatedConfiguration(configUpdateMsg.getTransid());
		updateRunninigWiFiSettings();
//		getRunningConfiguration(deviceID);
//		updateRunningConfigSettings();
//		
//		updPrevRunningConfigUpdates(); // scan for previously running values from CONFIG_UPDATES table.
		
		configUpdates.setStatus(ConfigUpdateStatus.RUNNING);
		configUpdates.setConfigAck(configUpdateMsg.toString());
		//configUpdates.setConfigAck(configUpdateMsg.getConfigStatusBytes().toByteArray());
		try {
			genDao.saveGenericObject(configUpdates);
		} catch (DAOException e) {
			logger.error("Exception occured while saving updated ConfiguredValuesMO entity.");
			return Boolean.FALSE;
		}
		return Boolean.TRUE;
	}
	
	/**
	 *  get the latest transaction for a device whose 
	 *  status is "Running" and change that configuration as In-Active.
	 */
	private void updPrevRunningConfigUpdates() {
		List<FilterCondition> cond = new ArrayList<>();
		List<ConfiguredValuesMO> prevConfigList = null;
		ConfiguredValuesMO  prevConfig = null;
		setFilterConditions(cond); 
		try {
			prevConfigList = (List<ConfiguredValuesMO>) genDao.getGenericObjects(ConfiguredValuesMO.class, cond, Boolean.TRUE);
		} catch (DAOException e) {
			logger.error("Exception while retrieving Config updates for the device.");
			e.printStackTrace();
		}
		prevConfig = prevConfigList.get(0); // Ideally this should give only one item.
		prevConfig.setStatus(ConfigUpdateStatus.INACTIVE);
		try {
			genDao.saveGenericObject(prevConfig);
		} catch (DAOException e) {
			e.printStackTrace();
		}
	}

	
	private void setFilterConditions(List<FilterCondition> cond) {
		QueryHelper.CreateQueryFilter("DEVICE_ID", configUpdates.getDeviceId(), ComparisonOperator.EQ);
		QueryHelper.CreateQueryFilter("STATUS", "Running", ComparisonOperator.EQ);
	
	}

	/**
	 * This method will get the updated configuration for the transactionID
	 * from CONFIG_UPDATE table.
	 * @param transactionID
	 */
	private void getUpdatedConfiguration(String transactionID) {
		// fetch the configuration.
		configUpdates = DeviceConfigurationAPI.queryConfigUpdatesTable(transactionID);
		updatedConfig = configUpdates.getConfiguration();
		if(updatedConfig == null) {
			logger.info("DeviceConfiguration is null.");
			return ;
		}
	}
	/**
	 * This method will get the RunningConfiguration for the devicdID
	 * 
	 * @param deviceId
	 */
	public void getRunningConfiguration(String deviceId) {
		// fetch the configuration.
		runningConfig = DeviceConfigurationAPI.fetchDeviceConfiguration(deviceId);
	}

	private void updateRunningConfigSettings() {
		
		if(updatedConfig.getLanSettings() != null) {
			updateRunninigLANSettings();
		}
		if(updatedConfig.getWanSetting() != null) {
			updateRunninigWANSettings();
		}
		if(updatedConfig.getRadioSettings() != null) {
			updateRunninigRadioSettings();
		}
		if(updatedConfig.getWifiSettings() != null) {
			updateRunninigWiFiSettings();
		}
		
		try {
			genDao.saveGenericObject(device);
		} catch (DAOException e) {
			logger.error("Exception while saving device entity. "+e.getCause().toString());
		}
		
	}

	private void updateRunninigRadioSettings() {
		
		DeviceRadioSettings runningRadioSetting = null;
		radioSettingsList = runningConfig.getRadioSettings();
		
		for(DeviceRadioSettings updRadioSetting: updatedConfig.getRadioSettings()) {
			
			runningRadioSetting = DeviceConfigurationAPI.getListItem(radioSettingsList, Integer.valueOf(updRadioSetting.getIndex()));
			
			if(runningRadioSetting == null) {
				return;
			}
			
			int index = radioSettingsList.indexOf(runningRadioSetting);
			
			if(RequestValidator.isValidString(updRadioSetting.getRadioMode())) 
				runningRadioSetting.setBabel(updRadioSetting.getRadioMode());
			
			if(RequestValidator.isValidString(updRadioSetting.getRadioStatus()))
				runningRadioSetting.setRadioStatus(updRadioSetting.getRadioStatus());
			
			if(RequestValidator.isValidString(updRadioSetting.getBandwidth()))
				runningRadioSetting.setBandwidth(updRadioSetting.getBandwidth());
				
			if(RequestValidator.isValidString(updRadioSetting.getChannel()))
				runningRadioSetting.setChannel(updRadioSetting.getChannel());
				
			if(RequestValidator.isValidString(updRadioSetting.getTransmitPower()))
				runningRadioSetting.setTransmitPower(updRadioSetting.getTransmitPower());	
			
			if(RequestValidator.isValidString(updRadioSetting.getRadioBand()))
				runningRadioSetting.setRadioBand(updRadioSetting.getRadioBand());		
			
			radioSettingsList.add(index, runningRadioSetting);
		}
		device.setRadioSettings(radioSettingsList);
	}

	private void updateRunninigWANSettings() {
		WANSettings updWanSetting = updatedConfig.getWanSetting();
		runningWanSetting = runningConfig.getWanSetting();
		
			
		if(RequestValidator.isValidString(updWanSetting.getDnsPrimary()))
			runningWanSetting.setDnsPrimary(updWanSetting.getDnsPrimary());
		
		if(RequestValidator.isValidString(updWanSetting.getDnsSecondary()))
			runningWanSetting.setDnsSecondary(updWanSetting.getDnsSecondary());
		
		if(RequestValidator.isValidString(updWanSetting.getGateway()))
			runningWanSetting.setGateway(updWanSetting.getGateway());
		
		if(RequestValidator.isValidString(updWanSetting.getIpv4()))
			runningWanSetting.setIpv4(updWanSetting.getIpv4());
		
		if(RequestValidator.isValidString(updWanSetting.getIpv6()))
			runningWanSetting.setIpv6(updWanSetting.getIpv6());
		
		if(RequestValidator.isValidString(updWanSetting.getSubnetMask()))
			runningWanSetting.setSubnetMask(updWanSetting.getSubnetMask());
		
		if(RequestValidator.isValidString(updWanSetting.getNat()))
			runningWanSetting.setNat(updWanSetting.getNat());
		
		if(RequestValidator.isValidString(updWanSetting.getUsername()))
			runningWanSetting.setUsername(updWanSetting.getUsername());
		
		if(RequestValidator.isValidString(updWanSetting.getPassword()))
			runningWanSetting.setPassword(updWanSetting.getPassword());
		
		device.setWanSettings(runningWanSetting);
		
	}

	private void updateRunninigLANSettings() {
		LANSettings runningLanSetting = null;
		lanSettingsList = DynamoDBHelper.getInstance().getLANSetting(runningConfig.getLanSettings());
		
		for(LANSettings updLanSetting: DynamoDBHelper.getInstance().getLANSetting(updatedConfig.getLanSettings())) {
			runningLanSetting = DeviceConfigurationAPI.getListItem(lanSettingsList, Integer.valueOf(updLanSetting.getIndex()));
			
			if(runningLanSetting == null) {
				return;
			}
			
			int index = lanSettingsList.indexOf(runningLanSetting);
			
			if(RequestValidator.isValidString(updLanSetting.getDhcp()))
				runningLanSetting.setDhcp(updLanSetting.getDhcp());
				
			if(RequestValidator.isValidString(updLanSetting.getDnsPrimary()))
				runningLanSetting.setDnsPrimary(updLanSetting.getDnsPrimary());
			
			if(RequestValidator.isValidString(updLanSetting.getDnsSecondary()))
				runningLanSetting.setDnsSecondary(updLanSetting.getDnsSecondary());
			
			if(RequestValidator.isValidString(updLanSetting.getGateway()))
				runningLanSetting.setGateway(updLanSetting.getGateway());
			
			if(RequestValidator.isValidString(updLanSetting.getIpV4()))
				runningLanSetting.setIpV4(updLanSetting.getIpV4());
			
			if(RequestValidator.isValidString(updLanSetting.getIpV6()))
				runningLanSetting.setIpV6(updLanSetting.getIpV6());
			
			if(RequestValidator.isValidString(updLanSetting.getSubnetMask()))
				runningLanSetting.setSubnetMask(updLanSetting.getSubnetMask());
			
			lanSettingsList.add(index, runningLanSetting);
		}
		device.setLanSettings(lanSettingsList);
		
	}

	private void updateRunninigWiFiSettings() {
		//WiFiSettings runningWifiSetting = null;
		//wifiSettingsList = runningConfig.getWifiSettings();
		logger.info("Going to fetch WIFI settings..");
		for(WiFiSettings updWiFiSetting: updatedConfig.getWifiSettings()) {
			
			//runningWifiSetting = DeviceConfigurationAPI.getListItem(wifiSettingsList, Integer.valueOf(updWiFiSetting.getIndex()));
			populateWiFI( updWiFiSetting);
			NetworkSettings nws = updWiFiSetting.getNetworkSettings();
			if(nws != null) {
				
				populateNetwork(nws);
			}
			
			//int index = wifiSettingsList.indexOf(runningWifiSetting);
			
		}
		
	}

	private void populateNetwork(NetworkSettings updNetworkSettings) {
		
		Network network = null;
		try {
			network = genDao.getGenericObject(Network.class, updNetworkSettings.getNetworkID());
		} catch (DAOException e) {
			logger.error("Exception while retrieving Network entity. "+e.getCause().toString());
		}
		
		if(RequestValidator.isValidString(updNetworkSettings.getDhcpServer()))
			network.setDhcpServer(updNetworkSettings.getDhcpServer());
		
		if(RequestValidator.isValidString(updNetworkSettings.getIpPoolEnd()))
			network.setIpPoolEnd(updNetworkSettings.getIpPoolEnd());
		
		if(RequestValidator.isValidString(updNetworkSettings.getIpPoolStart()))
			network.setIpPoolStart(updNetworkSettings.getIpPoolStart());
		
		if(RequestValidator.isValidString(updNetworkSettings.getLeaseTime()))
			network.setLeaseTime(updNetworkSettings.getLeaseTime());
		
		if(RequestValidator.isValidString(updNetworkSettings.getZtEnable()))
			network.setZtEnable(updNetworkSettings.getZtEnable());
		
		try {
			genDao.saveGenericObject(network);
		} catch (DAOException e) {
			logger.error("Exception while saving Network entity. "+e.getCause().toString());
		}
		
	}

	private void populateWiFI(WiFiSettings updWiFiSetting) {
		
		WiFi wifi = null;
		
		try {
			wifi = genDao.getGenericObject(WiFi.class, updWiFiSetting.getWifiID());
			logger.info("GOT exact WiFi record from DB.");
		} catch (DAOException e) {
			logger.error("Exception while retrieving WiFi entity. "+e.getCause().toString());
		}
		
		if(RequestValidator.isValidString(updWiFiSetting.getAuthenticationType()))
			wifi.setAuthenticationType(updWiFiSetting.getAuthenticationType());
		
		if(RequestValidator.isValidString(updWiFiSetting.getEapType()))
			wifi.setEapType(updWiFiSetting.getEapType());
		
		if(RequestValidator.isValidString(updWiFiSetting.getEnableWireless()))
			wifi.setEnableWireless(updWiFiSetting.getEnableWireless());
		
		if(RequestValidator.isValidString(updWiFiSetting.getEncryptionType()))
			wifi.setEncryptionType(updWiFiSetting.getEncryptionType());
		
		if(RequestValidator.isValidString(updWiFiSetting.getHiddenSSID()))
			wifi.setHiddenSSID(updWiFiSetting.getHiddenSSID());
		
		if(RequestValidator.isValidString(updWiFiSetting.getSsid()))
			wifi.setSsid(updWiFiSetting.getSsid());
		
		if(RequestValidator.isValidString(updWiFiSetting.getMode()))
			wifi.setMode(updWiFiSetting.getMode());
		
		if(RequestValidator.isValidString(updWiFiSetting.getPassPhrase()))
			wifi.setPassPhrase(updWiFiSetting.getPassPhrase());
		
		
		try {
			genDao.saveGenericObject(wifi);
			logger.info("Saved Wifi data to DB.");
		} catch (DAOException e) {
			logger.error("Exception while saving WiFi entity. "+e.getCause().toString());
		}
			
	}

}
