/**
 * @author Kanimozhi.M
 *
 * @Dated 21-Jun-2018  15:14:55 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.ProtoBufMsgProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.generate.protobuf;

import org.apache.log4j.Logger;

/**
 * @author Kanimozhi.M
 *
 * @Dated 21-Jun-2018  15:14:55 PM
 *
 */
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.framework.MessageProtocolFactory;
import com.wio.common.protobuf.impl.WioLogProtoData;
import com.wio.common.protobuf.model.WIOLogProtoMO;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class LogProtoBufMsgProducer 
{
	private static final Logger logger = Logger.getLogger(LogProtoBufMsgProducer.class);

	public WioLogProtoData constructProtoBufMsg() 
	{
		WioLogProtoData proto = null;
		IMessageSerializer protocolType = MessageProtocolFactory.getFactoryInst().getMessageProtocol(WioKafkaConstants.PROTPBUF);
		proto = (WioLogProtoData) protocolType.getMesgSerializer(WioKafkaConstants.WIO_LOG_PROTO);
		proto.messageConstructor();
		WIOLogProtoMO wioLogProtoMO =new WIOLogProtoMO();
		wioLogProtoMO.setAction("log");
		wioLogProtoMO.setDeviceId("06aa74-f801-a401-b6ytgheb7117");
		wioLogProtoMO.setDftopic("/wio/india/south/act/my_apartments");
		wioLogProtoMO.setLog("Wed Jun 14 10:19:36 2017 daemon.info hostapd: wlan0: STA c4:0b:cb:54:47:2f WPA: pairwise key handshake completed (WPA)");
		wioLogProtoMO.setLogfile("xxx");
		wioLogProtoMO.setTime("Thu Jun 22 05:14:24 2017");
		proto.setConfigUpdateDetails(wioLogProtoMO);
		proto.serialize();
		logger.info("After Serialize == "+proto);
		return proto;
	}
}
