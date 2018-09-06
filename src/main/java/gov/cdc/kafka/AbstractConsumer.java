package gov.cdc.kafka;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.google.gson.Gson;

import gov.cdc.helper.OAuthHelper;
import gov.cdc.helper.ResourceHelper;
import gov.cdc.kafka.common.WorkerException;
import gov.cdc.security.SSLCertificateValidation;

public abstract class AbstractConsumer implements Runnable {

	private static final Logger logger = Logger.getLogger(AbstractConsumer.class);

	protected final KafkaConsumer<String, GenericRecord> consumer;
	protected final String incomingTopicName;
	protected final String outgoingTopicName;
	protected final String errorTopicName;
	protected final String kafkaBrokers;
	protected boolean oauth_enabled;

	public AbstractConsumer() {
		throw new IllegalAccessError("The empty constructor should not be used.");
	}

	public AbstractConsumer(String groupName, String incomingTopicName, String outgoingTopicName, String errorTopicName, String kafkaBrokers, String schemaRegistryUrl) throws IOException {
		this.incomingTopicName = incomingTopicName;
		this.outgoingTopicName = outgoingTopicName;
		this.errorTopicName = errorTopicName;
		this.kafkaBrokers = kafkaBrokers;

		// Prepare properties for the consumer
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaBrokers);
		props.put("auto.commit.enable", false);
		props.put("key.deserializer", ResourceHelper.getProperty("kafka.consumer." + getProfileName().toLowerCase() + ".key.deserializer"));
		props.put("value.deserializer", ResourceHelper.getProperty("kafka.consumer." + getProfileName().toLowerCase() + ".value.deserializer"));
		if (!StringUtils.isEmpty(schemaRegistryUrl))
			props.put("schema.registry.url", schemaRegistryUrl);
		props.put("group.id", groupName);

		// Create the consumer
		this.consumer = new KafkaConsumer<String, GenericRecord>(props);

		// Checking the list of topics
		logger.debug("Checking the list of topics...");
		Map<String, List<PartitionInfo>> topics = consumer.listTopics();
		logger.debug("   ... Found " + topics.keySet().size() + " topic(s):");
		for (String topicName : topics.keySet()) {
			logger.debug("   ...... " + topicName);
		}
		if (topics.keySet().contains(incomingTopicName))
			logger.debug("   ... The following topic is available: " + incomingTopicName);
		else
			throw new IOException("The following topic is not available: " + incomingTopicName);

		// Check if we need to disable SSL verification
		logger.debug("Checking if we need to disable SSL...");
		try {
			boolean disableSSL = Boolean.parseBoolean(ResourceHelper.getSysEnvProperty("SSL_VERIFYING_DISABLE", true));
			logger.debug("   ... " + (disableSSL ? "Yes" : "No"));
			if (disableSSL)
				SSLCertificateValidation.disable();
		} catch (Exception e) {
			logger.error(e);
		}

		// Check if oauth is enabled
		logger.debug("Checking if we need to enable OAuth...");
		oauth_enabled = false;
		try {
			oauth_enabled = Boolean.parseBoolean(ResourceHelper.getSysEnvProperty("OAUTH_ENABLED", true));
			logger.debug("   ... " + (oauth_enabled ? "Yes" : "No"));
		} catch (Exception e) {
			logger.error(e);
		}
	}

	public void run() {
		logger.debug("Starting...");
		try {
			consumer.subscribe(Collections.singletonList(incomingTopicName));
			logger.debug("Subscribed to: " + incomingTopicName);

			while (true) {
				ConsumerRecords<String, ?> records = consumer.poll(Long.MAX_VALUE);
				consumer.commitSync();
				for (ConsumerRecord<String, ?> record : records) {
					process(record);
				}
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			consumer.close();
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}

	public void handleError(String profileName, Exception e, String objectId) {
		logger.error("Error in the profile " + profileName + " [ id = `" + objectId + "` ]");
		logger.error(e);
		String exc = (new Gson()).toJson(e);
		JSONObject json = new JSONObject();
		json.put("exception", new JSONObject(exc));
		json.put("profile", profileName);
		json.put("id", objectId);
		try {
			sendErrorMessage(json.toString());
		} catch (Exception e1) {
			logger.error(e1);
		}
	}

	protected abstract void process(ConsumerRecord<String, ?> record);

	protected abstract String getProfileName();

	private void sendErrorMessage(String data) throws IOException, WorkerException {
		if (StringUtils.isEmpty(errorTopicName))
			throw new WorkerException("Impossible to send a message if the error topic name is not configured!");
		sendMessage(data, errorTopicName);
	}

	public void sendMessage(String data) throws IOException, WorkerException {
		if (StringUtils.isEmpty(outgoingTopicName))
			throw new WorkerException("Impossible to send a message if the outgoing topic name is not configured!");
		sendMessage(data, outgoingTopicName);
	}

	private void sendMessage(String data, String topic) throws IOException, WorkerException {
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaBrokers);
		props.put("acks", "all");
		props.put("retries", 3);

		props.put("key.serializer", ResourceHelper.getProperty("kafka.producer.key.serializer"));
		props.put("value.serializer", ResourceHelper.getProperty("kafka.producer.value.serializer"));

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		logger.debug("Sending message to: " + topic);
		producer.send(new ProducerRecord<String, String>(topic, "message", data));
		logger.debug("... Sent!");
		producer.close();
	}

	public boolean isOauthEnabled() {
		return oauth_enabled;
	}

	public String getAuthorizationHeader() throws Exception {
		if (isOauthEnabled()) {
			return "Bearer " + OAuthHelper.getInstance().getToken();
		} else
			return null;
	}

}