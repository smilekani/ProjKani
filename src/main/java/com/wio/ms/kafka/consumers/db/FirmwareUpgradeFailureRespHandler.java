/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  16:15:50 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.db.FirmwareUpgradeFailureRespHandler.java
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
import com.wio.common.config.model.ConfiguredValuesMO;
import com.wio.common.exception.DAOException;
import com.wio.common.generic.dao.DDBGenericDAO;
import com.wio.common.protobuf.generated.ConfigUpdatesProto.ConfigUpdates;

public class FirmwareUpgradeFailureRespHandler 
{
	private DDBGenericDAO genDao = DDBGenericDAO.getInstance();
	private static final Logger logger = Logger.getLogger(FirmwareUpgradeFailureRespHandler.class);
	
	public FirmwareUpgradeFailureRespHandler() {
	}
	
	public Boolean processFailureResp(ConfigUpdates configUpdateMsg) {
		ConfiguredValuesMO configUpdates;
		try {
			configUpdates = genDao.getGenericObject(ConfiguredValuesMO.class, configUpdateMsg.getTransid());
			configUpdates.setStatus(ConfigUpdateStatus.FIRMWARE_UPGRADE_FAILED);
			configUpdates.setConfigAck(configUpdateMsg.getStatusBytes().toString());
			genDao.saveGenericObject(configUpdates);
		} catch (DAOException e1) {
			logger.error("Exception occured while saving updated ConfiguredValuesMO entity.");
			return Boolean.FALSE;
		}
		return Boolean.TRUE;
	}
}
