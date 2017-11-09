package hortonworks.hdf.sam.custom.processor.enrich.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.common.exception.ConfigException;
import com.hortonworks.streamline.streams.Result;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

/**
 * A Phoenix based enrichment processor. The processor will execute a 
 * user provided sql statement and the results of those queries will be mapped
 * to fields provided by the user in the enrichedOuputFields arg. 
 *SQL statements can refer to the values from the input schema using the placeholder ${field}
 * 
 * Examples of sql statement include: 
 * 1. Single statement: select certified, wage_plan
 * from drivers where driverid=${driverId}; 
 * 
 * 2.select hours_logged, miles_logged from timesheet where driverid= ${driverId} and
 * week=${week}";
 * 
 * Supports both secure and unsecure Phoneix/HBase Clusters
 * 
 * @author gvetticaden
 *
 */
public class PhoenixEnrichmentSecureProcessor implements CustomProcessorRuntime {

	
	protected static final Logger LOG = LoggerFactory
			.getLogger(PhoenixEnrichmentSecureProcessor.class);

	static final String CONFIG_ZK_SERVER_URL = "zkServerUrl";
	static final String CONFIG_ENRICHMENT_SQL = "enrichmentSQL";
	static final String CONFIG_ENRICHED_OUTPUT_FIELDS = "enrichedOutputFields";
	
	static final String CONFIG_SECURE_CLUSTER = "secureCluster";
	static final String CONFIG_KERBEROS_CLIENT_PRINCIPAL = "kerberosClientPrincipal";
	static final String CONFIG_KERBEROS_KEYTAB_FILE = "kerberosKeyTabFile";

	
	/* This is temporary work around so we can have unique output stream names. Right now the SAM won't allow the same 2 or more output stream to have the same name*/
	//private static final String CONFIG_OUTPTUT_STREAM_SUFFIX = "outputStreamSuffix";

	/* TODO: GJVETT: You should probably create a pool of connections */
	private Connection phoenixConnection = null;
	private String enrichmentSQLStatement = null;
	private String[] enrichedOutPutFields;
	private boolean secureCluster;
	
	//private String outputStreamName;
	

	public void cleanup() {
		DbUtils.closeQuietly(phoenixConnection);

	}
	


	/**
	 * Initializing the JDBC connection to Phoenix
	 */
	public void initialize(Map<String, Object> config) {
		LOG.info("Initializing + " + PhoenixEnrichmentSecureProcessor.class.getName());

		
		this.enrichmentSQLStatement =  ((String) config.get(CONFIG_ENRICHMENT_SQL)).trim();
		LOG.info("The configured enrichment SQL is: " + enrichmentSQLStatement);

		String outputFields = (String) config
				.get(CONFIG_ENRICHED_OUTPUT_FIELDS);
		String outputFieldsCleaned = StringUtils.deleteWhitespace(outputFields);
		this.enrichedOutPutFields = outputFieldsCleaned.split(",");
		LOG.info("Enriched Output fields is: " + enrichedOutPutFields);
		
		LOG.info("Seure Cluster Flag is: " + config.get(CONFIG_SECURE_CLUSTER));
		LOG.info("Kerberos Principal is: " + config.get(CONFIG_KERBEROS_CLIENT_PRINCIPAL));
		LOG.info("Kerberos KeyTab is: " + config.get(CONFIG_KERBEROS_KEYTAB_FILE));		
		
		setUpJDBCPhoenixConnection(config);		

	}


	/**
	 * Enrich the event with teh results from the user provided sql queries
	 */
	public List<StreamlineEvent> process(StreamlineEvent event)
			throws ProcessingException {
		LOG.info("Event[" + event + "] about to be enriched");

		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
		builder.putAll(event);

		/* Enrich */
		Map<String, Object> enrichValues = enrich(event);
		LOG.info("Enriching events[" + event
				+ "]  with the following enriched values: " + enrichValues);
		builder.putAll(enrichValues);

		/* Build the enriched streamline event and return */
		List<Result> results = new ArrayList<Result>();
		StreamlineEvent enrichedEvent = builder.dataSourceId(
				event.getDataSourceId()).build();
		LOG.info("Enriched StreamLine Event is: " + enrichedEvent);

		List<StreamlineEvent> newEvents = Collections
				.<StreamlineEvent> singletonList(enrichedEvent);

		return newEvents;
	}

	/**
	 * Returns a map of the the results of the sql queries where the key is the
	 * schema input field name and the value is the enriched value
	 * 
	 * @param event
	 * @return
	 */
	private Map<String, Object> enrich(StreamlineEvent event) {


		Map<String, Object> enrichedValues = new HashMap<String, Object>();

		StrSubstitutor strSub = new StrSubstitutor(event);

		String enrichSQLToExecute = strSub.replace(this.enrichmentSQLStatement);
		ResultSet rst = null;
		Statement statement = null;
		try {

			LOG.info("The SQL with substitued fields to be executed is: "
					+ enrichSQLToExecute);

			statement = phoenixConnection.createStatement();
			rst = statement.executeQuery(enrichSQLToExecute);

			if (rst.next()) {
				int columnCount = rst.getMetaData().getColumnCount();
				for (int i = 1, count=0; i <= columnCount; i++) {
					enrichedValues.put(enrichedOutPutFields[count++],
							rst.getString(i));
				}
			} else {
				String errorMsg = "No results found for enrichment query: "
						+ enrichSQLToExecute;
				LOG.error(errorMsg);
				throw new RuntimeException(errorMsg);
			}
		} catch (SQLException e) {
			String errorMsg = "Error enriching event[" + event
					+ "] with enrichment sql[" + this.enrichmentSQLStatement + "]";
			LOG.error(errorMsg, e);
			throw new RuntimeException(errorMsg, e);

		} finally {
			DbUtils.closeQuietly(rst);
			DbUtils.closeQuietly(statement);

		}

		return enrichedValues;
	}

	@Override
	public void validateConfig(Map<String, Object> config) throws ConfigException {
		boolean isSecureCluster =  config.get(CONFIG_SECURE_CLUSTER) != null && ((Boolean) config.get(CONFIG_SECURE_CLUSTER)).booleanValue();
		if(isSecureCluster) {
			String principal = (String) config.get(CONFIG_KERBEROS_CLIENT_PRINCIPAL);
			String keyTab  = (String) config.get(CONFIG_KERBEROS_KEYTAB_FILE);
			if(StringUtils.isEmpty(principal) || StringUtils.isEmpty(keyTab)) {
				throw new ConfigException("If Secure Cluster, Kerberos principal and key tabe must be provided");
			}
		}

	}



	private String constructInSecureJDBCPhoenixConnectionUrl(String zkServerUrl) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("jdbc:phoenix:").append(zkServerUrl)
				.append(":/hbase-unsecure");
		return buffer.toString();
	}
	
	//jdbc:phoenix:zk_quorum:2181:/hbase-secure:hbase@EXAMPLE.COM:/hbase-secure/keytab/keytab_file

	private String constructSecureJDBCPhoenixConnectionUrl(String zkServerUrl, String clientPrincipal, String keyTabFile) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("jdbc:phoenix:").append(zkServerUrl)
				.append(":/hbase-secure")
				.append(":").append(clientPrincipal)
				.append(":").append(keyTabFile);
		return buffer.toString();
	}	
	
	

	
	private void setUpJDBCPhoenixConnection(Map<String, Object> config) {
		String zkServerUrl = (String) config.get(CONFIG_ZK_SERVER_URL);
		
		

	
		boolean secureCluster = config.get(CONFIG_SECURE_CLUSTER) != null && ((Boolean)config.get(CONFIG_SECURE_CLUSTER)).booleanValue();
				
		String clientPrincipal = (String)config.get(CONFIG_KERBEROS_CLIENT_PRINCIPAL);
		String keyTabFile =  (String)config.get(CONFIG_KERBEROS_KEYTAB_FILE);		
		
		String jdbcPhoenixConnectionUrl = "";
		if(secureCluster) {
			jdbcPhoenixConnectionUrl = constructSecureJDBCPhoenixConnectionUrl(zkServerUrl, clientPrincipal, keyTabFile);
		} else {
			jdbcPhoenixConnectionUrl = constructInSecureJDBCPhoenixConnectionUrl(zkServerUrl);
		}
		
		LOG.info("Initializing Phoenix Connection with JDBC connection string["
				+ jdbcPhoenixConnectionUrl + "]");
		try {
			phoenixConnection = DriverManager
					.getConnection(jdbcPhoenixConnectionUrl);
		} catch (SQLException e) {
			String error = "Error creating Phoenix JDBC connection";
			LOG.error(error, e);
			throw new RuntimeException(error);
		}
		LOG.info("Successfully created Phoenix Connection with JDBC connection string["
				+ jdbcPhoenixConnectionUrl + "]");
	}	
	

}
