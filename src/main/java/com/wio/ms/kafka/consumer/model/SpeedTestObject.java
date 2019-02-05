/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  14:55:53 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.consumers.SpeedTestObject.java
 *
 *
 */
package com.wio.ms.kafka.consumer.model;

/**
 * @author Kanimozhi.M
 *
 * @Dated 06-Jul-2018  14:55:53 PM
 *
 */

public class SpeedTestObject {

	private String deviceid;
	private String duration;
	private String server_id;
	private String server_location;
	private String event;
	private String local_time;
	private String speed;
	private String speed_type;
	private String start_time;
	private String value;

	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getDeviceid() {
		return deviceid;
	}
	public void setDeviceid(String deviceid) {
		this.deviceid = deviceid;
	}
	public String getEvent() {
		return event;
	}
	public void setEvent(String event) {
		this.event = event;
	}
	public String getLocal_time() {
		return local_time;
	}
	public void setLocal_time(String local_time) {
		this.local_time = local_time;

	}
	public String getSpeed() {
		return speed;
	}
	public void setSpeed(String speed) {
		this.speed = speed;
	}
	public String getSpeed_type() {
		return speed_type;
	}
	public void setSpeed_type(String speed_type) {
		this.speed_type = speed_type;
	}
	public String getStart_time() {
		return start_time;
	}
	public void setStart_time(String start_time) {
		this.start_time = start_time;
	}
	public String getDuration() {
		return duration;
	}
	public void setDuration(String duration) {
		this.duration = duration;
	}
	public String getServer_id() {
		return server_id;
	}
	public void setServer_id(String server_id) {
		this.server_id = server_id;
	}
	public String getServer_location() {
		return server_location;
	}
	public void setServer_location(String server_location) {
		this.server_location = server_location;
	}
	/*@Override
	public String toString() {
		return "MqttMsgObject [deviceid=" + deviceid + ", duration=" + duration + ", server_id=" + server_id
				+ ", server_location=" + server_location + ", event=" + event + ", local_time=" + local_time
				+ ", speed=" + speed + ", speed_type=" + speed_type + ", start_time=" + start_time + ", value=" + value
				+ "]";
	}*/

	@Override
	public String toString() {
		return "{\"deviceid\":\""+deviceid+"\",\"event\":\""+event+"\",\"local_time\":\""+local_time+"\",\"speed_type\":\""+speed_type+
				"\",\"value\":\""+value+"\"}";


	}

}
