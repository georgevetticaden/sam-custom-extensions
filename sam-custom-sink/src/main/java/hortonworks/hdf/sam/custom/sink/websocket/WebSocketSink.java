package hortonworks.hdf.sam.custom.sink.websocket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import com.hortonworks.streamline.streams.Result;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketSink implements CustomProcessorRuntime {
	
	protected static final Logger LOG = LoggerFactory.getLogger(WebSocketSink.class);
	private static final long ACTIVEMQ_MESSAGE_TTL = 10000;	
	
	private static final String CONFIG_CONNECTION_USER_ID= "connectionUserId";
	private static final String CONFIG_CONNECTION_PASSWORD="connectionPassword";
	private static final String CONFIG_CONNECTION_URL="connectionUrl";
	private static final String CONFIG_TOPIC_NAME = "websocketTopicName";
	
	private String user;
	private String password;
	private String activeMQConnectionString;
	private String topicName;
	
	private Session session = null;
	private Connection connection = null;
	private ActiveMQConnectionFactory connectionFactory = null;
    private HashMap<String, MessageProducer> producers = new HashMap<String, MessageProducer>();	

    
	public List<StreamlineEvent> process(StreamlineEvent event)
			throws ProcessingException {
		
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("StreamLine event's field values are: " + event.getAuxiliaryFieldsAndValues());
		}
		 
		JSONObject json = new JSONObject();
        json.putAll(event.getAuxiliaryFieldsAndValues());	
        
        String message = json.toJSONString();
        puhblishMessage(message);
        
        List<StreamlineEvent> result = new ArrayList<StreamlineEvent>();
        return result;
	}

	public void initialize(Map<String, Object> config) {
	
		LOG.info("Initializing WebSocketSink custom processing");
		this.user = (String) config.get(CONFIG_CONNECTION_USER_ID);
		this.password = (String) config.get(CONFIG_CONNECTION_PASSWORD);
		this.activeMQConnectionString = (String) config.get(CONFIG_CONNECTION_URL);
		this.topicName = (String) config.get(CONFIG_TOPIC_NAME);
		
		initializeJMSConnection();
		
		LOG.info("Finished Initializing WebSocketSink custom processing");
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void validateConfig(Map<String, Object> config)
			throws ConfigException {
		// TODO Auto-generated method stub

	}
	
	private void initializeJMSConnection() {
		try{
			
			LOG.info("Starting to initialize JMS connection..");
			
			connectionFactory = new ActiveMQConnectionFactory(user, password,activeMQConnectionString);
			connection = connectionFactory.createConnection();
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            producers.put(this.topicName, getTopicProducer(session, this.topicName));
            
            LOG.info("JMS connection to topic["+topicName +"] successful");
		}
		catch (JMSException e) {
			LOG.error("Error sending to topic["+this.topicName + "]", e);
			return;
		}
	}
	
	private MessageProducer getTopicProducer(Session session, String topic) {
		try {
			Topic topicDestination = session.createTopic(topic);
			MessageProducer topicProducer = session.createProducer(topicDestination);
			topicProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			return topicProducer;
		} catch (JMSException e) {
			LOG.error("Error creating producer for topic", e);
			throw new RuntimeException("Error creating producer for topic");
		}
	}
	
	private void puhblishMessage(String event) {
		try {
            TextMessage message = session.createTextMessage(event);
			//getTopicProducer(sessio   n, topic).send(message);
			MessageProducer producer = producers.get(this.topicName);
			producer.send(message, producer.getDeliveryMode(), producer.getPriority(), ACTIVEMQ_MESSAGE_TTL);
		} catch (JMSException e) {
			LOG.error("Error sending event["+event + "]  to topic["+this.topicName+"]", e);
			return;
		}
	}	
	

}
