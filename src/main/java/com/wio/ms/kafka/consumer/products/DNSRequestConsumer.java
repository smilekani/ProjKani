/**
 * @author Kanimozhi.M
 *
 * @Dated 17-Jul-2018  18:42:41 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.DNSRequestConsumer.java
 *
 *
 */
package com.wio.ms.kafka.consumer.products;

/**
 * @author Kanimozhi.M
 *
 * @Dated 17-Jul-2018  18:42:41 PM
 *
 */
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.generated.WIODnsRequestProto.DNSREQUEST;
import com.wio.common.protobuf.impl.WioDNSRequestProtoData;
import com.wio.ms.kafka.consumer.factory.WioConsumer;
import com.wio.ms.kafka.proto.DNSProtoProcessing;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class DNSRequestConsumer extends WioConsumer {

	private static final Logger logger = Logger.getLogger(DNSRequestConsumer.class);

	public DNSRequestConsumer(String groupId, String topic, KafkaConsumer<byte[], byte[]> consumer) {
		super(groupId, topic, consumer);
	}

	@Override
	public void processRecords(IMessageSerializer protocolType, ConsumerRecord<byte[], byte[]> record) {
		logger.debug("Started Consumer processing of the DNS Request msgs.....");
		long startTime = System.currentTimeMillis();
		WioDNSRequestProtoData proto = (WioDNSRequestProtoData) protocolType.getMesgSerializer(WioKafkaConstants.WIO_DNS_PROTO);
		DNSREQUEST deserializedDNSObj = (DNSREQUEST) proto.deSerialize(record.value());
		logger.debug("Deserialized DNS Object : "+deserializedDNSObj);
		DNSProtoProcessing.getInstance().process(deserializedDNSObj);
		long endTime = System.currentTimeMillis();
		long timeTaken = endTime - startTime;
		logger.info("Time Taken by DNSRequestConsumer to process the DNSRequest is  : "+timeTaken);
	}

}
