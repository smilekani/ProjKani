/**
 * @author Kanimozhi.M
 *
 * @Dated 14-Jun-2018  16:50:48 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.proto.FingerPrintProtoProcessing.java
 *
 *
 */
package com.wio.ms.kafka.proto;

/**
 * @author Kanimozhi.M
 *
 * @Dated 14-Jun-2018  16:50:48 PM
 *
 */
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.wio.common.Enums.Constants.ClientDeviceAccess;
import com.wio.common.Enums.Constants.ClientDeviceType;
import com.wio.common.Enums.DynamoDBObjectKeys;
import com.wio.common.config.model.ClientDevice;
import com.wio.common.config.model.Device;
import com.wio.common.exception.DAOException;
import com.wio.common.generic.dao.FilterCondition;
import com.wio.common.generic.dao.GenericDAO;
import com.wio.common.generic.dao.GenericDAOFactory;
import com.wio.common.generic.dao.QueryHelper;
import com.wio.common.protobuf.generated.WIOEventsDhcpProto.DHCP;
import com.wio.common.util.UniqueIDGenerator;

public class FingerPrintProtoProcessing 
{
	private static GenericDAO genDAO = null;

	private static final Logger log = Logger.getLogger(FingerPrintProtoProcessing.class);

	private static FingerPrintProtoProcessing fingerPrintProtoProcessing;

	private FingerPrintProtoProcessing() 
	{
	}

	public static FingerPrintProtoProcessing getInstance()
	{
		if(fingerPrintProtoProcessing == null)
		{
			synchronized (FingerPrintProtoProcessing.class) 
			{
				fingerPrintProtoProcessing = new FingerPrintProtoProcessing();
			}
		}
		return fingerPrintProtoProcessing;
	}

	public void process(DHCP dhcp) {

		if(!dhcp.getAction().equalsIgnoreCase("NEW")) {
			return;
		}

		genDAO = GenericDAOFactory.getGenericDAO();
		log.info("Going to save client devices .");
		ClientDevice clientDevice=new ClientDevice();

		try {
			// Temporary fix: This condition added because if AP is restarted, it is sending all connected devices in eventsDHCP
			// which would be received by FingerprintMS. This needs to be prevented in notifying to mobile app and saving in DB.

			List<FilterCondition> filter =new ArrayList<FilterCondition>();
			filter.add(QueryHelper.CreateQueryFilter(DynamoDBObjectKeys.MAC_ADDRESS,dhcp.getDcmac() , ComparisonOperator.EQ));
			List<ClientDevice> clientDevList = genDAO.getGenericObjects(ClientDevice.class, filter);
			if(clientDevList != null && clientDevList.size() > 0) {
				return;
			}

			Device device = genDAO.getGenericObject(Device.class, dhcp.getDeviceid());
			if(device!=null ) {
				clientDevice.setDgId(device.getDgID());
			}
			clientDevice.setClientDeviceId("CD"+UniqueIDGenerator.getID());
			clientDevice.setApDeviceId(dhcp.getDeviceid());
			clientDevice.setMacAddress(dhcp.getDcmac());
			clientDevice.setName(dhcp.getDcidname());
			clientDevice.setIpAddress(dhcp.getDcip());
			clientDevice.setAccess(ClientDeviceAccess.NO_ACCESS);
			clientDevice.setDeviceType(ClientDeviceType.Unknown);
			clientDevice.setStatus("PENDING");

			log.info("Saved client device :: "+clientDevice.toString());
			genDAO.saveGenericObject(clientDevice);
		} catch (DAOException e) {
			log.error("Exception Occurred while Pushing the FingerPrint ES... ", e);
			e.printStackTrace();
		} 
		// Publish to mobile app 
		log.info("publishing to notify mobile topic .");


		/** for the Below , Kafka Implement to be TODO.

		/*JMSPublisher pub = new JMSPublisher(NOTIFY_MOBILE_APP_TOPIC + "/" + dhcp.getDeviceid());
		pub.setMessage(new Gson().toJson(clientDevice).toString());
		String response = pub.publishProtobufMesg(NOTIFY_MOBILE_APP_CLIENT);
		System.out.println("Notified "+response);*/

	}
}
