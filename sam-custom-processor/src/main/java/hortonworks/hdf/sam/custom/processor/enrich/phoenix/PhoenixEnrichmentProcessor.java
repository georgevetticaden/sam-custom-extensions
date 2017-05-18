package hortonworks.hdf.sam.custom.processor.enrich.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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

import com.hortonworks.streamline.streams.Result;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
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
 * @author gvetticaden
 *
 */
public class PhoenixEnrichmentProcessor implements CustomProcessorRuntime {

	private static final String OUTPUT_STREAM_NAME_BASE = "enrich_stream_";
	
	protected static final Logger LOG = LoggerFactory
			.getLogger(PhoenixEnrichmentProcessor.class);

	private static final String CONFIG_ZK_SERVER_URL = "zkServerUrl";
	private static final String CONFIG_ENRICHMENT_SQL = "enrichmentSQL";
	private static final String CONFIG_ENRICHED_OUTPUT_FIELDS = "enrichedOutputFields";
	
	/* This is temporary work around so we can have unique output stream names. Right now the SAM won't allow the same 2 or more output stream to have the same name*/
	private static final String CONFIG_OUTPTUT_STREAM_SUFFIX = "outputStreamSuffix";

	/* TODO: GJVETT: You should probably create a pool of connections */
	private Connection phoenixConnection = null;
	private String enrichmentSQLStatement = null;
	private String[] enrichedOutPutFields;
	private String outputStreamName;
	

	public void cleanup() {
		DbUtils.closeQuietly(phoenixConnection);

	}

	/**
	 * Initializing the JDBC connection to Phoenix
	 */
	public void initialize(Map<String, Object> config) {
		LOG.info("Initializing + " + PhoenixEnrichmentProcessor.class.getName());

		String outputStreamSuffix = (String) config.get(CONFIG_OUTPTUT_STREAM_SUFFIX);
		this.outputStreamName = OUTPUT_STREAM_NAME_BASE + outputStreamSuffix;
		LOG.info("The output stream name is: " + this.outputStreamName);
		
		this.enrichmentSQLStatement =  ((String) config.get(CONFIG_ENRICHMENT_SQL)).trim();
		LOG.info("The configured enrichment SQL is: " + enrichmentSQLStatement);

		String outputFields = (String) config
				.get(CONFIG_ENRICHED_OUTPUT_FIELDS);
		String outputFieldsCleaned = StringUtils.deleteWhitespace(outputFields);
		this.enrichedOutPutFields = outputFieldsCleaned.split(",");
		LOG.info("Enriched Output fields is: " + enrichedOutPutFields);

		setUpJDBCPhoenixConnection(config);

	}


	/**
	 * Enrich the event with teh results from the user provided sql queries
	 */
	public List<Result> process(StreamlineEvent event)
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

		results.add(new Result(this.outputStreamName, newEvents));
		return results;
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

	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		// TODO Auto-generated method stub

	}



	private String constructJDBCPhoenixConnectionUrl(String zkServerUrl) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("jdbc:phoenix:").append(zkServerUrl)
				.append(":/hbase-unsecure");
		return buffer.toString();
	}
	
	private void setUpJDBCPhoenixConnection(Map<String, Object> config) {
		String zkServerUrl = (String) config.get(CONFIG_ZK_SERVER_URL);
		String jdbcPhoenixConnectionUrl = constructJDBCPhoenixConnectionUrl(zkServerUrl);
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
