/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  16:15:50 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.db.FirmwareUpgradeSuccessRespHandler.java
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
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.framework.MessageProtocolFactory;
import com.wio.common.protobuf.generated.ConfigUpdatesProto.ConfigUpdates;
import com.wio.common.protobuf.impl.ConfigProtoData;

public class FirmwareUpgradeSuccessRespHandler {
	private ConfiguredValuesMO configUpdates = null;
	private static final Logger logger = Logger.getLogger(FirmwareUpgradeSuccessRespHandler.class);

	private DDBGenericDAO genDao = DDBGenericDAO.getInstance();

	public Boolean processSuccessResp(ConfigUpdates configUpdateMsg) {
		IMessageSerializer protocolType = MessageProtocolFactory.getFactoryInst().getMessageProtocol("PROTPBUF");
		ConfigProtoData proto = (ConfigProtoData) protocolType.getMesgSerializer("CONFIG_UPDATE");
		proto.messageConstructor();

		try {
			configUpdates = genDao.getGenericObject(ConfiguredValuesMO.class, configUpdateMsg.getTransid());
			configUpdates.setStatus(ConfigUpdateStatus.FIRMWARE_UPGRADE_SUCCESS);
			configUpdates.setConfigAck(configUpdateMsg.toString());
			genDao.saveGenericObject(configUpdates);
		} catch (DAOException e) {
			logger.error("Exception occured while saving updated ConfiguredValuesMO entity.");
			return Boolean.FALSE;
		}
		return Boolean.TRUE;
	}
}
