/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  16:15:50 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.db.FirmwareDownloadFailureRespHandler.java
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

public class FirmwareDownloadFailureRespHandler 
{
	private static final Logger logger = Logger.getLogger(FirmwareDownloadFailureRespHandler.class);
	private DDBGenericDAO genDao = DDBGenericDAO.getInstance();

	public Boolean processFailureResp(ConfigUpdates configUpdateMsg)
	{
		ConfiguredValuesMO configUpdates;
		try {
			configUpdates = genDao.getGenericObject(ConfiguredValuesMO.class, configUpdateMsg.getTransid());
			configUpdates.setStatus(ConfigUpdateStatus.FIRMWARE_DOWNLOAD_FAILED);
			configUpdates.setConfigAck(configUpdateMsg.getStatusBytes().toString());
			genDao.saveGenericObject(configUpdates);
		} catch (DAOException e1) {
			logger.error("Exception occured while saving updated ConfiguredValuesMO entity."+e1);
			e1.printStackTrace();
			return Boolean.FALSE;
		}
		return Boolean.TRUE;
	}

}
