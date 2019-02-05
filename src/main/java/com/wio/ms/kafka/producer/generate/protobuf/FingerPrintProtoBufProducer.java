/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.FingerPrintProtoBufProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.generate.protobuf;

import org.apache.log4j.Logger;

/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 */
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.framework.MessageProtocolFactory;
import com.wio.common.protobuf.impl.WIOEventsDhcpProtoData;
import com.wio.common.protobuf.model.WIOEventsDhcpProtoMO;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class FingerPrintProtoBufProducer {

	private static final Logger logger = Logger.getLogger(FingerPrintProtoBufProducer.class);
	
	public WIOEventsDhcpProtoData constructProtoBufMsg() 
	{
		IMessageSerializer protocolType = MessageProtocolFactory.getFactoryInst().getMessageProtocol(WioKafkaConstants.PROTPBUF);
		WIOEventsDhcpProtoData proto = (WIOEventsDhcpProtoData) protocolType.getMesgSerializer(WioKafkaConstants.DHCP_EVENT);
		proto.messageConstructor();
		WIOEventsDhcpProtoMO wioEventsDhcpProtoMO = constructDhcpObject();
		proto.setConfigUpdateDetails(wioEventsDhcpProtoMO);
		proto.serialize();
		logger.info("After speedTest serialize == "+proto);
		return proto;
	}

	private WIOEventsDhcpProtoMO constructDhcpObject() {
		WIOEventsDhcpProtoMO wioEventsDhcpProtoMO = new WIOEventsDhcpProtoMO();
		wioEventsDhcpProtoMO.setEvent("event");
		wioEventsDhcpProtoMO.setTime("Friday");
		wioEventsDhcpProtoMO.setDeviceid("deviceid");
		wioEventsDhcpProtoMO.setType("type");
		wioEventsDhcpProtoMO.setAction("action");
		wioEventsDhcpProtoMO.setDexpiry("dexpiry");
		wioEventsDhcpProtoMO.setDcmac("dcmac");
		wioEventsDhcpProtoMO.setDcip("dcip");
		wioEventsDhcpProtoMO.setDcidname("dcidname");
		wioEventsDhcpProtoMO.setDcid("dcid");
		wioEventsDhcpProtoMO.setDftopic("dftopic");
		return wioEventsDhcpProtoMO;
	}

}
