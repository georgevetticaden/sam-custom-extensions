package hortonworks.hdf.sam.custom.processor.enrich.phoenix;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hortonworks.hdf.sam.custom.processor.enrich.phoenix.PhoenixEnrichmentSecureProcessor;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.junit.Test;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PhoenixEnrichmentSecureProcessorTest {

	private static final String HR_ENRICHMENT_SQL = "select certified, wage_plan from DRIVERS where driverid=${driverId}";
	private static final String HR_OUTPUT_FIELDS = "driverCertification, driverWagePlan";
	protected static final Logger LOG = LoggerFactory.getLogger(PhoenixEnrichmentSecureProcessorTest.class);
		
	
	private static final String UNSECURE_ZK_SRVER_URL = "secure-sam-hdf0.field.hortonworks.com:2181";
	private static final String SECURE_ZK_SRVER_URL = "sam-hdf0.field.hortonworks.com:2181";
	
	private static final String KERBEROS_CLIENT_PRINCIPAL = "storm-streamanalytics@STREAMANALYTICS";
	private static final String KERBEROS_KEY_TAB_FILE = "/Users/gvetticaden/Dropbox/Hortonworks/FIELD-CLOUD/sam-hdf/storm.headless.keytab";
	                                
	
	@Test
	public void testHREnrichmentNonSecureCluster() throws Exception {
		PhoenixEnrichmentSecureProcessor enrichProcessor = new PhoenixEnrichmentSecureProcessor();
		Map<String, Object> processorConfig = createHREnrichmentConfig(false);
		enrichProcessor.validateConfig(processorConfig);
		enrichProcessor.initialize(processorConfig);
		
		List<StreamlineEvent> eventResults = enrichProcessor.process(createStreamLineEvent());
		
		LOG.info("Result of enrichment is: " + ReflectionToStringBuilder.toString(eventResults));
		
		
	}
	
	@Test
	public void testHREnrichmentSecureCluster() throws Exception {
		PhoenixEnrichmentSecureProcessor enrichProcessor = new PhoenixEnrichmentSecureProcessor();
		Map<String, Object> processorConfig = createHREnrichmentConfig(true);
		enrichProcessor.validateConfig(processorConfig);
		enrichProcessor.initialize(processorConfig);
		
		List<StreamlineEvent> eventResults = enrichProcessor.process(createStreamLineEvent());
		
		LOG.info("Result of enrichment is: " + ReflectionToStringBuilder.toString(eventResults));
		
		
	}

		

	private StreamlineEvent createStreamLineEvent() {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		keyValues.put("driverId", 14);
		
		//StreamlineEventImpl event = new StreamlineEventImpl(keyValues, "1");
		StreamlineEvent event = StreamlineEventImpl.builder().build().addFieldsAndValues(keyValues);
		
		System.out.println("Iinput StreamLIne event is: " + ReflectionToStringBuilder.toString(event));

		
		return event;
	}

	private Map<String, Object> createHREnrichmentConfig(boolean secure) {
		Map<String, Object> processorConfig = new HashMap<String, Object>();
		
		processorConfig.put(PhoenixEnrichmentSecureProcessor.CONFIG_ENRICHMENT_SQL, HR_ENRICHMENT_SQL);
		processorConfig.put(PhoenixEnrichmentSecureProcessor.CONFIG_ENRICHED_OUTPUT_FIELDS, HR_OUTPUT_FIELDS);
		
		if(secure) {
			processorConfig.put(PhoenixEnrichmentSecureProcessor.CONFIG_SECURE_CLUSTER, true);
			processorConfig.put(PhoenixEnrichmentSecureProcessor.CONFIG_ZK_SERVER_URL, SECURE_ZK_SRVER_URL);
			processorConfig.put(PhoenixEnrichmentSecureProcessor.CONFIG_KERBEROS_CLIENT_PRINCIPAL, KERBEROS_CLIENT_PRINCIPAL);
			processorConfig.put(PhoenixEnrichmentSecureProcessor.CONFIG_KERBEROS_KEYTAB_FILE, KERBEROS_KEY_TAB_FILE);
			
		} else {
			processorConfig.put(PhoenixEnrichmentSecureProcessor.CONFIG_ZK_SERVER_URL, UNSECURE_ZK_SRVER_URL);
		}
				
		return processorConfig;
	}
	
	
}
