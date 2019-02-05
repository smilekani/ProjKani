/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.EventProtoBufProducer.java
 *
 *
 */
package com.wio.ms.kafka.producer.generate.protobuf;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * @author Kanimozhi.M
 *
 * @Dated 12-Jul-2018  18:15:00 PM
 *
 */
import com.wio.common.protobuf.framework.IMessageSerializer;
import com.wio.common.protobuf.framework.MessageProtocolFactory;
import com.wio.common.protobuf.impl.WIOEventProtoData;
import com.wio.common.protobuf.model.WIOEventsArpProtoMO;
import com.wio.common.protobuf.model.WIOEventsDhcpProtoMO;
import com.wio.common.protobuf.model.WIOEventsHostapdProtoMO;
import com.wio.common.protobuf.model.WIOEventsMEMProtoMO;
import com.wio.common.protobuf.model.WIOEventsWANProtoMO;
import com.wio.common.protobuf.model.WIOEventsMEMProtoMO.WIOEventMEMORY;
import com.wio.common.protobuf.model.WIOEventsMEMProtoMO.WIOEventMemSWAP;
import com.wio.ms.kafka.utils.WioKafkaConstants;

public class EventProtoBufProducer {

	private static final Logger logger = Logger.getLogger(EventProtoBufProducer.class);

	public WIOEventProtoData constructProtoBufMsg() 
	{
		IMessageSerializer protocolType = MessageProtocolFactory.getFactoryInst().getMessageProtocol(WioKafkaConstants.PROTPBUF);
		WIOEventProtoData proto = (WIOEventProtoData) protocolType.getMesgSerializer(WioKafkaConstants.WIO_EVENT_PROTO);
		proto.messageConstructor();
		proto.serializeOneOf(3);

		// ARP 
		WIOEventsArpProtoMO wioEventsArpProtoMO = constructARPObject();
		proto.setConfigUpdateDetails(wioEventsArpProtoMO);

		// DHCP 
		WIOEventsDhcpProtoMO wioEventsDhcpProtoMO = constructDhcpObject();
		proto.setConfigUpdateDetails(wioEventsDhcpProtoMO);

		// AP 
		WIOEventsHostapdProtoMO wioEventsApProtoMO = constructAPObject();
		proto.setConfigUpdateDetails(wioEventsApProtoMO);

		// WAN 
		WIOEventsWANProtoMO wioEventsWanProtoMO = constructWANObject();
		proto.setConfigUpdateDetails(wioEventsWanProtoMO);
		
		// MEM 
		WIOEventsMEMProtoMO wioEventsMemProtoMO = constructMEMObject();
		proto.setConfigUpdateDetails(wioEventsMemProtoMO);

		logger.info("wioData.serialize()==>> "+proto.serialize().getAllFields());
		proto.serialize();
		logger.info("After speedTest serialize == "+proto);
		return proto;
	}
	
	private WIOEventsMEMProtoMO constructMEMObject() {
		WIOEventsMEMProtoMO wioEventsMemProtoMO = new WIOEventsMEMProtoMO();
		wioEventsMemProtoMO.setEvent("MEM");
		wioEventsMemProtoMO.setTime("1212");
		wioEventsMemProtoMO.setDeviceid("MEM deviceId1");
		wioEventsMemProtoMO.setLocaltime("localtime");
		wioEventsMemProtoMO.setUptime("uptime");
		List<String> loadList = new ArrayList<>();
		loadList.add("Load1");
		loadList.add("Load2");
		loadList.add("Load3");
		loadList.add("Load4");
		loadList.add("Load5");
		wioEventsMemProtoMO.setLoad(loadList);
		WIOEventMEMORY memory = new WIOEventMEMORY();
		memory.setTotal("total");
		memory.setFree("free");
		memory.setShared("shared");
		memory.setBuffered("buffered");
		wioEventsMemProtoMO.setMemory(memory);
		WIOEventMemSWAP swap = new WIOEventMemSWAP();
		swap.setTotal("total");
		swap.setFree("free");
		wioEventsMemProtoMO.setSwap(swap );
		wioEventsMemProtoMO.setDftopic("dftopic");
		return wioEventsMemProtoMO;
	}

	private WIOEventsWANProtoMO constructWANObject() {
		WIOEventsWANProtoMO wioEventsWanProtoMO = new WIOEventsWANProtoMO();
		wioEventsWanProtoMO.setEvent("WAN");
		wioEventsWanProtoMO.setTime("191");
		wioEventsWanProtoMO.setDeviceid("WAN deviceId1");
		wioEventsWanProtoMO.setUp("up");
		wioEventsWanProtoMO.setPending("pending");
		wioEventsWanProtoMO.setAvailable("available");
		wioEventsWanProtoMO.setAutostart("autostart");
		wioEventsWanProtoMO.setDynamic("dynamic");
		wioEventsWanProtoMO.setUptime("uptime");
		wioEventsWanProtoMO.setL3device("l3device");
		wioEventsWanProtoMO.setProto("proto");
		wioEventsWanProtoMO.setDevice("device");
		wioEventsWanProtoMO.setIpv4addr("ipv4addr");
		wioEventsWanProtoMO.setIpv4netmask("ipv4netmask");
		wioEventsWanProtoMO.setIpv6addr("ipv6addr");
		wioEventsWanProtoMO.setIpv6netmask("ipv6netmask");
		wioEventsWanProtoMO.setDftopic("dftopic");
		return wioEventsWanProtoMO;
	}

	private WIOEventsHostapdProtoMO constructAPObject() {
		WIOEventsHostapdProtoMO wioEventsApProtoMO = new WIOEventsHostapdProtoMO();
		wioEventsApProtoMO.setEvent("Host AP");
		wioEventsApProtoMO.setTime("151");
		wioEventsApProtoMO.setDeviceid("AP deviceId1");
		wioEventsApProtoMO.setWlanmode("wlanmode");
		wioEventsApProtoMO.setIface("iface");
		wioEventsApProtoMO.setSeverity("severity");
		wioEventsApProtoMO.setEventinfo("eventinfo");
		wioEventsApProtoMO.setDftopic("dftopic");
		return wioEventsApProtoMO;
	}

	private WIOEventsDhcpProtoMO constructDhcpObject() {
		WIOEventsDhcpProtoMO wioEventsDhcpProtoMO = new WIOEventsDhcpProtoMO();
		wioEventsDhcpProtoMO.setEvent("DHCP");
		wioEventsDhcpProtoMO.setTime("121");
		wioEventsDhcpProtoMO.setDeviceid("DHCP deviceId1");
		wioEventsDhcpProtoMO.setType("Type DHCP");
		wioEventsDhcpProtoMO.setAction("ARP Event Action");
		wioEventsDhcpProtoMO.setDexpiry("dexpiry");
		wioEventsDhcpProtoMO.setDcmac("dcmac");
		wioEventsDhcpProtoMO.setDcip("dcip");
		wioEventsDhcpProtoMO.setDcidname("dcidname");
		wioEventsDhcpProtoMO.setDcid("dcid");
		wioEventsDhcpProtoMO.setDftopic("dftopic");
		return wioEventsDhcpProtoMO;
	}

	private WIOEventsArpProtoMO constructARPObject() {
		WIOEventsArpProtoMO wioEventsArpProtoMO = new WIOEventsArpProtoMO();
		wioEventsArpProtoMO.setEvent("ARP");
		wioEventsArpProtoMO.setTime("313");
		wioEventsArpProtoMO.setDeviceid("ARP deviceId1");
		wioEventsArpProtoMO.setType("Type-ARP");
		wioEventsArpProtoMO.setAction("ARP Event Action");
		wioEventsArpProtoMO.setIpaddress("1921");
		wioEventsArpProtoMO.setHwtype("ARP HW");
		wioEventsArpProtoMO.setFlag("true");
		wioEventsArpProtoMO.setMacaddress("macaddress");
		wioEventsArpProtoMO.setMask("ARPmask");
		wioEventsArpProtoMO.setDevice("ARP device");
		wioEventsArpProtoMO.setDftopic("ARPdftopic");
		return wioEventsArpProtoMO;
	}

}
