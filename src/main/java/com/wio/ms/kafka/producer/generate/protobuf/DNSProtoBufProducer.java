/**
 * @author Kanimozhi.M
 *
 * @Dated 19-Jul-2018  12:16:40 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.DNSProtoBufProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.generate.protobuf;

import org.apache.log4j.Logger;

/**
 * @author Kanimozhi.M
 *
 * @Dated 19-Jul-2018  12:16:40 PM
 *
 */
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.framework.MessageProtocolFactory;
import com.wio.common.protobuf.generated.WIODnsRequestProto.DNSREQUEST;
import com.wio.common.protobuf.impl.WioDNSRequestProtoData;
import com.wio.common.protobuf.model.WIODNSProtoMO;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class DNSProtoBufProducer {

	private static final Logger logger = Logger.getLogger(DNSProtoBufProducer.class);
	
	public WioDNSRequestProtoData constructProtoBufMsg() 
	{
		IMessageSerializer protocolType = MessageProtocolFactory.getFactoryInst().getMessageProtocol(WioKafkaConstants.PROTPBUF);
		WioDNSRequestProtoData proto = (WioDNSRequestProtoData) protocolType.getMesgSerializer(WioKafkaConstants.WIO_DNS_PROTO);
		proto.messageConstructor();
		WIODNSProtoMO wioDNSProtoMO = new WIODNSProtoMO();
		wioDNSProtoMO.setId("id");
		wioDNSProtoMO.setSite("site");
		wioDNSProtoMO.setParental_control(1);
		wioDNSProtoMO.setClient_mac("client_mac");
		wioDNSProtoMO.setApuuid("apuuid");
		wioDNSProtoMO.setDefault_topic("default_topic");
		wioDNSProtoMO.setSeverity(1);
		proto.setConfigUpdateDetails(wioDNSProtoMO);
		proto.serialize();
		DNSREQUEST output = proto.deSerialize(proto.getWioLogByteString().toByteArray());

		logger.info("De-Serialize out ==> "+output);
		logger.info("===========>>"+proto.serialize().getAllFields());
		return proto;


	}

}
