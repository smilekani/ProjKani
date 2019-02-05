/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  16:15:50 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.db.ConfigFailureRespHandler.java
 *
 *
 */
package com.wio.ms.kafka.consumers.db;

import org.apache.log4j.Logger;

/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  16:15:50 PM
 *
 */
import com.wio.common.Enums.Constants.ConfigUpdateStatus;
import com.wio.common.config.api.DeviceConfigurationAPI;
import com.wio.common.config.model.ConfiguredValuesMO;
import com.wio.common.config.request.DeviceConfiguration;
import com.wio.common.exception.DAOException;
import com.wio.common.generic.dao.DDBGenericDAO;
import com.wio.common.protobuf.generated.ConfigUpdatesProto.ConfigUpdates;

public class ConfigFailureRespHandler {

	private ConfiguredValuesMO configUpdates = null;
	private DeviceConfiguration updatedConfig = null;

	private DDBGenericDAO genDao = DDBGenericDAO.getInstance();
	
	private static final Logger logger = Logger.getLogger(ConfigFailureRespHandler.class);

	public ConfigFailureRespHandler() {

	}

	public Boolean processFailureResp(ConfigUpdates configUpdateMsg) {

		getUpdatedConfiguration(configUpdateMsg.getTransid());
		configUpdates.setStatus(ConfigUpdateStatus.FAILED);
		configUpdates.setConfigAck(configUpdateMsg.getStatusBytes().toString());
		try {
			genDao.saveGenericObject(configUpdates);
		} catch (DAOException e) {
			//e.printStackTrace();
			logger.info("Exception occured while saving updated ConfiguredValuesMO entity.");
			return Boolean.FALSE;
		}
		return Boolean.TRUE;
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


}
