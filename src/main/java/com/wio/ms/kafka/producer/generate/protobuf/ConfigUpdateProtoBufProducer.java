/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jul-2018  16:37:07 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.ConfigUpdateProtoBufProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.generate.protobuf;

import org.apache.log4j.Logger;

/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jul-2018  16:37:07 PM
 *
 */
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.framework.MessageProtocolFactory;
import com.wio.common.protobuf.impl.ConfigProtoData;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class ConfigUpdateProtoBufProducer {

	private static final Logger logger = Logger.getLogger(ConfigUpdateProtoBufProducer.class);
	
	public ConfigProtoData constructProtoBufMsg() 
	{
		IMessageSerializer protocolType = MessageProtocolFactory.getFactoryInst().getMessageProtocol(WioKafkaConstants.PROTPBUF);
		ConfigProtoData proto = (ConfigProtoData) protocolType.getMesgSerializer(WioKafkaConstants.CONFIG_UPDATE_PROTO);
		proto.messageConstructor();
		proto.setConfigUpdateDetails("Config", "12:20", "06aa74-f801-a401-b6ytgheb7117", "Success", "ffc3e01a-dad1-4b76-b307-fc0453a73d4e");
		proto.setConfigParamDetails("key1", "1");
		proto.setConfigParamDetails("key2", "2");
		proto.serialize();
		String output = proto.deSerializeToString(proto.getConfigUpdateByteString().toByteArray());

		logger.info("De-Serialize out ==> "+output);
		logger.info("===========>>"+proto.serialize().getAllFields());
		return proto;


	}

}
