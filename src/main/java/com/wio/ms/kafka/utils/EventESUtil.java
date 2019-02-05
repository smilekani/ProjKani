/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jun-2018  18:39:00 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.utils.EsUtil.java
 *
 *
 */
package com.wio.ms.kafka.utils;

import com.wio.common.aws.api.es.event.model.HostApdESMO;
import com.wio.common.aws.api.es.event.model.HostArpESMO;
import com.wio.common.aws.api.es.event.model.HostDhcpESMO;
import com.wio.common.aws.api.es.event.model.HostMemESMO;
import com.wio.common.aws.api.es.event.model.HostMemESMO.HostMemSwapESMO;
import com.wio.common.aws.api.es.event.model.HostMemESMO.HostMemoryESMO;
import com.wio.common.aws.api.es.event.model.HostWanESMO;
import com.wio.common.protobuf.generated.WIOEventProto.APEVENT;
import com.wio.common.protobuf.generated.WIOEventProto.ARP;
import com.wio.common.protobuf.generated.WIOEventProto.DHCP;
import com.wio.common.protobuf.generated.WIOEventProto.MEMSTATUS;
import com.wio.common.protobuf.generated.WIOEventProto.WANSTATUS;
import com.wio.common.validation.RequestValidator;

public class EventESUtil {

	public static HostArpESMO jsonForEs(ARP arp) {
		HostArpESMO hostArpESMO = new HostArpESMO();
		hostArpESMO.setEvent(arp.getEvent());
		hostArpESMO.setTime(KafkaConfUtil.timestampToUtc(arp.getTime()));
		hostArpESMO.setDeviceid(arp.getDeviceid());
		hostArpESMO.setType(arp.getType());
		hostArpESMO.setEventinfo(arp.getAction());
		hostArpESMO.setIpaddress(arp.getIpaddress());
		hostArpESMO.setHwtype(arp.getHwtype());
		hostArpESMO.setFlag(arp.getFlag());
		hostArpESMO.setCmac(arp.getMacaddress());
		hostArpESMO.setMask(arp.getMask());
		hostArpESMO.setDevice(arp.getDevice());
		hostArpESMO.setDevicegroup(arp.getDftopic());

		return hostArpESMO;
	}

	public static HostDhcpESMO jsonForEs(DHCP dhcp) {
		HostDhcpESMO hostDhcpESMO = new HostDhcpESMO();
		hostDhcpESMO.setEvent(dhcp.getEvent());
		hostDhcpESMO.setTime(KafkaConfUtil.timestampToUtc(dhcp.getTime()));
		hostDhcpESMO.setDeviceid(dhcp.getDeviceid());
		hostDhcpESMO.setType(dhcp.getType());
		hostDhcpESMO.setEventinfo(dhcp.getAction());
		hostDhcpESMO.setExpiry(dhcp.getDexpiry());
		hostDhcpESMO.setCmac(dhcp.getDcmac());
		hostDhcpESMO.setCip(dhcp.getDcip());
		hostDhcpESMO.setCidname(dhcp.getDcidname());
		hostDhcpESMO.setCid(dhcp.getDcid());
		hostDhcpESMO.setDevicegroup(dhcp.getDftopic());

		return hostDhcpESMO;
	}

	public static HostApdESMO jsonForEs(APEVENT apevent) {
		HostApdESMO hostApdESMO = new HostApdESMO();
		hostApdESMO.setDevicegroup(apevent.getDftopic());
		hostApdESMO.setDeviceid(apevent.getDeviceid());
		hostApdESMO.setEvent(apevent.getEvent());

		if(RequestValidator.isEmptyField(apevent.getEventinfo().split(" ")[1])) {
			hostApdESMO.setEventinfo(apevent.getEventinfo());
			hostApdESMO.setCmac("");
		}else {
			hostApdESMO.setEventinfo(apevent.getEventinfo().split(" ")[0]);
			hostApdESMO.setCmac(apevent.getEventinfo().split(" ")[1]);
		}

		hostApdESMO.setIface(apevent.getIface());
		hostApdESMO.setSeverity(apevent.getSeverity());
		hostApdESMO.setTime(KafkaConfUtil.timestampToUtc(apevent.getTime()));
		hostApdESMO.setWlanMode(apevent.getWlanmode());
		return hostApdESMO;
	}

	public static HostWanESMO jsonForEs(WANSTATUS wan) {
		HostWanESMO hostWanESMO = new HostWanESMO();
		hostWanESMO.setEvent(wan.getEvent());
		hostWanESMO.setTime(KafkaConfUtil.timestampToUtc(wan.getTime()));
		hostWanESMO.setDeviceid(wan.getDeviceid());
		hostWanESMO.setUp(Boolean.valueOf(wan.getUp()));
		hostWanESMO.setPending(Boolean.valueOf(wan.getPending()));
		hostWanESMO.setAvailable(Boolean.valueOf(wan.getAvailable()));
		hostWanESMO.setAutostart(Boolean.valueOf(wan.getAutostart()));
		hostWanESMO.setDynamic(Boolean.valueOf(wan.getDynamic()));
		hostWanESMO.setUptime(Long.parseLong(wan.getUptime()));
		hostWanESMO.setL3device(wan.getL3Device());
		hostWanESMO.setProto(wan.getProto());
		hostWanESMO.setDevice(wan.getDevice());
		hostWanESMO.setIpv4addr(wan.getIpv4Address());
		hostWanESMO.setIpv4netmask(wan.getIpv4Netmask());
		hostWanESMO.setIpv6addr(wan.getIpv6Address());
		hostWanESMO.setIpv6netmask(wan.getIpv6Netmask());
		hostWanESMO.setDevicegroup(wan.getDftopic());

		return hostWanESMO;
	}
	
	public static HostMemESMO jsonForEs(MEMSTATUS mem) {
		HostMemESMO hostMemESMO = new HostMemESMO();
		hostMemESMO.setEvent(mem.getEvent());
		hostMemESMO.setTime(KafkaConfUtil.timestampToUtc(mem.getTime()));
		hostMemESMO.setDeviceid(mem.getDeviceid());
		hostMemESMO.setUptime(mem.getUptime());
		hostMemESMO.setLoad(mem.getLoadList());
		HostMemoryESMO memory = new HostMemoryESMO();
		memory.setTotal(mem.getMemory().getTotal());
		memory.setFree(mem.getMemory().getFree());
		memory.setShared(mem.getMemory().getShared());
		memory.setBuffered(mem.getMemory().getBuffered());
		hostMemESMO.setMemory(memory);
		HostMemSwapESMO swap = new HostMemSwapESMO();
		swap.setTotal(mem.getSwap().getTotal());
		swap.setFree(mem.getSwap().getFree());
		hostMemESMO.setSwap(swap);
		hostMemESMO.setDevicegroup(mem.getDftopic());
		return hostMemESMO;
	}
}
