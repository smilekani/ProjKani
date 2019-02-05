/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jun-2018  16:32:56 PM
 *
 * @Email Kanimozhi.Mani@infinite.com
 *
 * @Project : Bayonette : wio
 *
 * @Location: com.wio.ms.kafka.proto.LogProtoProcessing.java
 *
 *
 */
package com.wio.ms.kafka.proto;

/**
 * @author Kanimozhi.M
 *
 * @Dated 13-Jun-2018  16:32:56 PM
 *
 */
import java.text.ParseException;
import java.util.Map;

import org.aicer.grok.dictionary.GrokDictionary;
import org.aicer.grok.util.Grok;
import org.apache.log4j.Logger;

import com.wio.common.aws.api.es.document.JestIndexDocument;
import com.wio.common.aws.api.es.log.model.LogESMO;
import com.wio.common.protobuf.generated.WIOLogProto.LOG;
import com.wio.ms.kafka.utils.KafkaConfUtil;

import io.searchbox.core.DocumentResult;

public class LogProtoProcessing 
{
	private static final Logger logger = Logger.getLogger(LogProtoProcessing.class);

	private static final String LOGSTASH_EXPRESSION = "^(?<timestamp>%{DAY} %{SPACE}%{MONTH} %{SPACE}%{MONTHDAY} %{SPACE}%{TIME} %{SPACE}%{YEAR})\\s+(%{WORD:module}.%{LOGLEVEL:loglevel}\\s+)+%{SYSLOGPROG:logsource}?%{GREEDYDATA:message}";

	private static LogProtoProcessing logProtoProcessing;

	private LogProtoProcessing() {
	}

	public static LogProtoProcessing getInstance()
	{
		if(logProtoProcessing == null)
		{
			synchronized (LogProtoProcessing.class) 
			{
				logProtoProcessing = new LogProtoProcessing();
			}
		}
		return logProtoProcessing;
	}


	public void process(LOG log) 
	{
		try
		{
			LogESMO logESModel=extractLogMessages(log);
			logger.info("Going to save in ES ."+logESModel);
			JestIndexDocument document = new JestIndexDocument(logESModel, KafkaConfUtil.LOG_INDEX, KafkaConfUtil.LOG_TYPE);
			DocumentResult result = document.indexDocument();
			logger.info("sucess: "+result.getJsonString());
		}catch (Exception e) {
			logger.error("Exception Occurred while Pushing the Log for ES... ", e);
			e.printStackTrace();
		}

	}

	private LogESMO extractLogMessages(LOG log) throws ParseException 
	{
		String logMessage=log.getLog();
		LogESMO logESMessagesObject = null;
		logger.info("In extractLogMessages method .");
		GrokDictionary dictionary = new GrokDictionary();
		dictionary.addBuiltInDictionaries();		
		dictionary.bind();	

		Grok compiledPattern = dictionary.compileExpression(LOGSTASH_EXPRESSION);

		String messages[] = logMessage.split("\n");

		for(String message : messages)
		{
			logESMessagesObject = getMessageObject(compiledPattern.extractNamedGroups(message.trim()),log);
			logger.info("The Log msg Object is : "+logESMessagesObject);
		}
		return logESMessagesObject;

	}

	private LogESMO getMessageObject(Map<String, String> extractNamedGroups, LOG log) throws ParseException 
	{
		LogESMO logESModel=new  LogESMO();

		if (extractNamedGroups != null) 
		{
			String apTime=KafkaConfUtil.timestampToUtc(extractNamedGroups.get("timestamp"));

			logESModel.setDeviceid(log.getDeviceid());
			logESModel.setTime(apTime);
			logESModel.setSeverity(extractNamedGroups.get("loglevel"));
			logESModel.setDevservice(extractNamedGroups.get("logsource"));
			String extractMsgs = extractNamedGroups.get("message");
			String newExtractMsg = extractMsgs.substring(extractMsgs.indexOf(':'));
			logESModel.setLog(newExtractMsg);
			logESModel.setDevicegroup(log.getDftopic());

		}
		return logESModel;
	}

}
