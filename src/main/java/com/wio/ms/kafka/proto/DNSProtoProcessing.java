/**
 * @author Kanimozhi.M
 *
 * @Dated 17-Jul-2018  18:42:41 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.proto.DNSProtoProcessing.java
 *
 *
 */
package com.wio.ms.kafka.proto;

/**
 * @author Kanimozhi.M
 *
 * @Dated 17-Jul-2018  18:42:41 PM
 *
 */
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.wio.common.aws.api.es.dns.model.DnsESMO;
import com.wio.common.aws.api.es.document.JestIndexDocument;
import com.wio.common.protobuf.generated.WIODnsRequestProto.DNSREQUEST;
import com.wio.ms.kafka.utils.KafkaConfUtil;

import io.searchbox.core.DocumentResult;

public class DNSProtoProcessing {
	private static final Logger logger = Logger.getLogger(DNSProtoProcessing.class);

	private static DNSProtoProcessing dnsProtoProcessing;

	private DNSProtoProcessing() {
	}

	public static DNSProtoProcessing getInstance()
	{
		if(dnsProtoProcessing == null)
		{
			synchronized (DNSProtoProcessing.class) 
			{
				dnsProtoProcessing = new DNSProtoProcessing();
			}
		}
		return dnsProtoProcessing;
	}


	public void process(DNSREQUEST dnsRequest) 
	{
		try
		{
			DnsESMO logESModel=getMessageObject(dnsRequest);
			logger.info("Going to save in ES ."+logESModel);
			JestIndexDocument document = new JestIndexDocument(logESModel, KafkaConfUtil.DNS_INDEX, KafkaConfUtil.DNS_TYPE);
			DocumentResult result = document.indexDocument();
			logger.info("success ES Push JSon in DNS : "+result.getJsonString());
		}catch (Exception e) {
			logger.error("Exception Occurred while Pushing the DNSRequest for ES... ", e);
			e.printStackTrace();
		}

	}

	private DnsESMO getMessageObject(DNSREQUEST dnsRequest) throws ParseException 
	{
		DnsESMO dnsESModel=new  DnsESMO();
		dnsESModel.setSite(dnsRequest.getSite());
		dnsESModel.setParental_control(String.valueOf(dnsRequest.getParentalControl()));
		dnsESModel.setClient_mac(dnsRequest.getClientMac());
		dnsESModel.setApuuid(dnsRequest.getApuuid());
		dnsESModel.setDefault_topic(dnsRequest.getDefaultTopic());
		dnsESModel.setSeverity(String.valueOf(dnsRequest.getSeverity()));
		return dnsESModel;
	}

}
