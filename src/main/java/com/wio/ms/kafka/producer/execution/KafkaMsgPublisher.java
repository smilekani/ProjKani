/**
 * @author Kanimozhi.M
 *
 * @Dated 21-Jun-2018  15:14:55 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.producers.KafkaMsgPublisher.java
 *
 *
 */
package com.wio.ms.kafka.producer.execution;

/**
 * @author Kanimozhi.M
 *
 * @Dated 21-Jun-2018  15:14:55 PM
 *
 */
import com.wio.ms.kafka.utils.KafkaConfUtil;

public class KafkaMsgPublisher 
{
	public static void main(String[] args) 
	{
//		KafkaMsgProducer.startProducer(KafkaConfUtil.WIO_LOG_TOPIC);
		KafkaMsgProducer.startProducer(KafkaConfUtil.WIO_RADIO_TOPIC);
		/*
		KafkaMsgProducer.startProducer(KafkaConfUtil.WIO_EVENT_TOPIC);
		KafkaMsgProducer.startProducer(KafkaConfUtil.WIO_CONFIG_UPDATE_TOPIC);
		KafkaMsgProducer.startProducer(KafkaConfUtil.WIO_FINGER_PRINT_TOPIC);
		KafkaMsgProducer.startProducer(KafkaConfUtil.WIO_SPEEDTEST_TOPIC);
		KafkaMsgProducer.startProducer(KafkaConfUtil.WIO_DNSREQUEST_TOPIC);*/
		
		
		/*	
		
/*		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
//		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
//		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<byte[], String> producer = new KafkaProducer<byte[], String>(props);*/
		
	/*	
		while (true)
		{
			TimeZone tz = TimeZone.getTimeZone("UTC");
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm.sss'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
			df.setTimeZone(tz);

			try {

				ProtoBufMsgProducer jmsMessageProducer = new ProtoBufMsgProducer();
				WioLogProtoData proto = jmsMessageProducer.constructProtoBufMsg();
				
				RadioProtoBufProducer radioProtoBufProducer = new RadioProtoBufProducer();
				WIORadioProtoData wioRadioProtoData = radioProtoBufProducer.constructProtoBufMsg();
				
				System.out.println("proto string : "+proto);
				
				final byte[] message = proto.getWioLogByteString().toByteArray();
				System.out.println("message : "+new String(message));

				kafkaProducer.send(new ProducerRecord<byte[], String>(KafkaConfUtil.WIO_LOG_TOPIC, new String(message)),	
				new Callback() 
				{
					public void onCompletion(RecordMetadata recordMetadata, Exception e) 
					{
						if ( e == null )
						{
							System.out.println("Partition: "+recordMetadata.partition()
							+", Offset" + recordMetadata.offset()
							+ ", timestamp: " + recordMetadata.timestamp());
							System.out.println(message);
						}
						else 
						{
							e.printStackTrace();
						}
					}
				});

				//				TimeUnit.SECONDS.sleep(1000);
			}
			catch ( Exception e   ){
				System.out.println("I was interrupted."+e);
			}
		}

	}*/
	}
}
