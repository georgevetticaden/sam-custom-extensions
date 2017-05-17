package hortonworks.hdf.sam.custom.processor.enrich;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * A Phoenix based enrichment processor. The processor will execute a series user provided sql statements and teh results of those queries
 * will be mapped to fields provided by the user in the enrichedOuputFields arg. Key things about the user provided sql statements:
 * 	1. Multiple sql statements can be provided where each statement is delimited by ';'
 * 	2. SQL statements can refer to the values from teh input schema using the placeholder ${field}
 * Examples of sql statement include: 
 * 	1. Single statement: select certified, wage_plan from drivers where driverid=${driverId};
 * 	2. Multiple statements: select certified, wage_plan from drivers where driverid=${driverId}; select hours_logged, miles_logged from timesheet where driverid= ${driverId} and week=${week}";
 * @author gvetticaden
 *
 */
public class PhoenixEnrichmentProcessor implements CustomProcessorRuntime {

	protected static final Logger LOG = LoggerFactory.getLogger(PhoenixEnrichmentProcessor.class);
	
	
	private static final String CONFIG_ZK_SERVER_URL="zkServerUrl";
	private static final String CONFIG_ENRICHMENT_SQL="enrichmentSQL";
	private static final String CONFIG_ENRICHED_OUTPUT_FIELDS = "enrichedOutputFields";


	private static final String SQL_DELIMITER = ";";

	
	private Connection phoenixConnection = null;
	private List<String> enrichmentSQLStatements = null;
	private String[] enrichedOutPutFields;
	
	public void cleanup() {
		try {
			phoenixConnection.close();
		} catch (SQLException e) {
			LOG.error("Error closing jdbc phoenix connection", e);
		}
		
	}

	public void initialize(Map<String, Object> config) {
		LOG.info("Initializing + " + PhoenixEnrichmentProcessor.class.getName());
		
		String enrichSQL = (String)config.get(CONFIG_ENRICHMENT_SQL);
		
		enrichmentSQLStatements = constructSQLStatements(enrichSQL);
		
		String outputFields = (String)config.get(CONFIG_ENRICHED_OUTPUT_FIELDS);
		String outputFieldsCleaned = StringUtils.deleteWhitespace(outputFields);
		this.enrichedOutPutFields = outputFieldsCleaned.split(",");
		
		LOG.info("Enriched Output fields is: " + enrichedOutPutFields);
		
		String zkServerUrl = (String)config.get(CONFIG_ZK_SERVER_URL);
		String jdbcPhoenixConnectionUrl = constructJDBCPhoenixConnectionUrl(zkServerUrl);
		LOG.info("Initializing Phoenix Connection with JDBC connection string["+ jdbcPhoenixConnectionUrl + "]");
		try {
			phoenixConnection = DriverManager.getConnection(jdbcPhoenixConnectionUrl);
		} catch (SQLException e) {
			String error = "Error creating Phoenix JDBC connection";
			LOG.error(error, e);
			throw new RuntimeException(error);
		}
		LOG.info("Successfully created Phoenix Connection with JDBC connection string["+ jdbcPhoenixConnectionUrl + "]");
		
	}

	private List<String> constructSQLStatements(String sql) {
		String[] sqlStatements = sql.split(SQL_DELIMITER);
		List<String> sqlStatementList = new ArrayList<String>();
		for(String sqlStatement: sqlStatements) {
			if(StringUtils.isNotBlank(sqlStatement)) 
				sqlStatementList.add(sqlStatement.trim());
		};
		return sqlStatementList;
	}

	private String constructJDBCPhoenixConnectionUrl(String zkServerUrl) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("jdbc:phoenix:")
			  .append(zkServerUrl)
			  .append(":/hbase-unsecure");
		return buffer.toString();
	}

	public List<Result> process(StreamlineEvent event) throws ProcessingException {
		LOG.info("Event["+ event+"] about to be enriched");
		
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
        builder.putAll(event);

        /* Enrich */
		Map<String, Object> enrichValues = enrich(event);
		LOG.info("Enriching events["+event + "]  with the following enriched values: " + enrichValues);
		builder.putAll(enrichValues);
		
        List<Result> results = new ArrayList<Result>();
        StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
        LOG.info("Enriched StreamLine Event is: " + enrichedEvent );
        List<StreamlineEvent> newEvents= Collections.<StreamlineEvent>singletonList(enrichedEvent);
        results.add(new Result("enrich_stream", newEvents));
        return results;        
	}

	private Map<String, Object> enrich(StreamlineEvent event) {
		Map<String, Object> enrichedValues = new HashMap<String, Object>();
		
		try {
			LOG.info("Number of SQL Queries going to be executed: " + enrichmentSQLStatements.size());
			StrSubstitutor strSub = new StrSubstitutor(event);
			int count = 0;
			
			for(String enrichmentSQL: enrichmentSQLStatements) {	
				
				String enrichSQLToExecute = strSub.replace(enrichmentSQL);
				
				LOG.info("The SQL with substitued fields to be executed is: " + enrichSQLToExecute);
				
				ResultSet rst = phoenixConnection.createStatement().
									executeQuery(enrichSQLToExecute);
				
				
				if(rst.next()) {
					int columnCount = rst.getMetaData().getColumnCount();
					for(int i=1;i<=columnCount;i++) {
						enrichedValues.put(enrichedOutPutFields[count++], rst.getString(i));
					}					
				} else {
					String errorMsg = "No results found for enrichment query: " + enrichSQLToExecute;
					LOG.error(errorMsg);
					throw new RuntimeException(errorMsg);
				}
				
			}
			
		} catch (SQLException e) {
			String errorMsg = "Error enriching event["+event+"] with info from Phoenix";
			LOG.error(errorMsg, e);
			throw new RuntimeException(errorMsg, e);
			
		}
					
		return enrichedValues;
	}

	


	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		// TODO Auto-generated method stub
		
	}
	

}
