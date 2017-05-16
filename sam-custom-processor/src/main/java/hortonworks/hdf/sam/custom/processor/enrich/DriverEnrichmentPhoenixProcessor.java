package hortonworks.hdf.sam.custom.processor.enrich;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

public class DriverEnrichmentPhoenixProcessor implements CustomProcessorRuntime {

	protected static final Logger LOG = LoggerFactory.getLogger(DriverEnrichmentPhoenixProcessor.class);
	
	private static final String DRIVER_ID_KEY = "driverId";
	private static final Object EVENT_TIME_KEY = "eventTime";
	
	private static final String FATIGUE_BY_HOURS_ENRICH_KEY="FatigueByHours";
	private static final String FATIGUE_BY_MILES_ENRICH_KEY="FatigueByMiles";
	private static final String FOGGY_WEATHER_ENRICH_KEY="FoggyWeather";
	private static final String RAINY_WEATHER_ENRICH_KEY="RainyWeather";
	private static final String WINDY_WEATHER_ENRICH_KEY="WindyWeather";
	
	private static final String CONFIG_ZK_SERVER_URL="zkServerUrl";
	private static final String CONFIG_ENRICHMENT_SQL="enrichmentSQL";
	private static final String CONFIG_ENRICHED_OUTPUT_FIELDS = "enrichedOutputFields";

	
	private Connection phoenixConnection = null;

	private String enrichmentSQL;

	private String[] enrichedOutPutFields;
	
	public void cleanup() {
		try {
			phoenixConnection.close();
		} catch (SQLException e) {
			LOG.error("Error closing jdbc phoenix connection", e);
		}
		
	}

	public void initialize(Map<String, Object> config) {
		LOG.info("Initializing + " + DriverEnrichmentPhoenixProcessor.class.getName());
		
		this.enrichmentSQL = (String)config.get(CONFIG_ENRICHMENT_SQL);
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

	private String constructJDBCPhoenixConnectionUrl(String zkServerUrl) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("jdbc:phoenix:")
			  .append(zkServerUrl)
			  .append(":/hbase-unsecure");
		return buffer.toString();
	}

	public List<Result> process(StreamlineEvent event) throws ProcessingException {
		Integer driverId = (Integer) event.get(DRIVER_ID_KEY);
		LOG.info("Driver["+driverId+"] about to be enriched");
		
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
        builder.putAll(event);
        
		if(driverId != null) {
			String eventTime = (String) event.get(EVENT_TIME_KEY);
			
			Map<String, Object> enrichedDriverInfo = enrich(event, driverId, eventTime);
			LOG.info("Enriching driver["+driverId + "]  with the following enriched values: " + enrichedDriverInfo);
			builder.putAll(enrichedDriverInfo);
			
		} else  {
			LOG.info("Skipping Enrichment because driverId was null..");
		}
        
        List<Result> results = new ArrayList<Result>();
        StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
        LOG.info("Enriched StreamLine Event is: " + enrichedEvent );
        List<StreamlineEvent> newEvents= Collections.<StreamlineEvent>singletonList(enrichedEvent);
        results.add(new Result("stream1", newEvents));
        return results;        
	}

	private Map<String, Object> enrich(StreamlineEvent event, Integer driverId, String eventTime) {
		Map<String, Object> enrichedValues = new HashMap<String, Object>();
		
		
		
		try {
			
			double certified=0, wageplan=0, hours_logged=0, miles_logged=0, foggy=0, rainy=0, windy=0;
			
			// get driver certification status and wage plan from hbase		
			StrSubstitutor strSub = new StrSubstitutor(event);
			String enrichSQLToExecute = strSub.replace(this.enrichmentSQL);
			
			LOG.info("The SQL with substitued fields to be executed is: " + enrichSQLToExecute);
			
			ResultSet rst = phoenixConnection.createStatement().
								executeQuery(enrichSQLToExecute);
			
			while (rst.next()) {
				certified = rst.getString(1).equals("Y") ? 1 : 0;
				wageplan =  rst.getString(1).equals("miles") ? 1 : 0;

			}
			
			enrichedValues.put(enrichedOutPutFields[0], certified);
			enrichedValues.put(enrichedOutPutFields[1], wageplan);			
			
			// get driver fatigue status from timesheet table in hbase 
			rst =	phoenixConnection.createStatement().
					executeQuery("select hours_logged, miles_logged from timesheet"
							+ " where driverid="+driverId+ " and week="+getWeek(eventTime));	
			
			while (rst.next()) {
				hours_logged = rst.getInt(1);
				miles_logged =  rst.getInt(2);

			}
			

			// scale the hours & miles features for spark model
			hours_logged = hours_logged/100;
			miles_logged = miles_logged/1000;
			

			enrichedValues.put(FATIGUE_BY_HOURS_ENRICH_KEY, hours_logged);
			enrichedValues.put(FATIGUE_BY_MILES_ENRICH_KEY,miles_logged);		
			
			enrichedValues.put(FOGGY_WEATHER_ENRICH_KEY, getFoggyWeatherConditions(driverId));
			enrichedValues.put(RAINY_WEATHER_ENRICH_KEY, getRainyWeatherConditions());
			enrichedValues.put(WINDY_WEATHER_ENRICH_KEY, getWindyWeatherConditions());			
			
			
		} catch (SQLException e) {
			String errorMsg = "Error enriching driver[" + driverId + "] with info from Phoenix Tables";
			LOG.error(errorMsg, e);
			throw new RuntimeException(errorMsg, e);
			
		}
					
		return enrichedValues;
	}

	private Object getWindyWeatherConditions() {
		double windy = new Random().nextInt(100) < 30 ? 1 : 0;
		return windy;
	}

	private Object getRainyWeatherConditions() {
		double rainy = new Random().nextInt(100) < 20 ? 1 : 0;;
		return rainy;
	}

	private Object getFoggyWeatherConditions(int driverID) {
		double foggy;
		if(driverID == 12) // jamie		
			foggy = new Random().nextInt(100) < 50 ? 1 : 0;
		else if (driverID == 11) // george
			foggy = new Random().nextInt(100) < 35 ? 1 : 0;
		else	
			foggy = new Random().nextInt(100) < 12 ? 1 : 0;
		return foggy;
	}


	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		// TODO Auto-generated method stub
		
	}
	
	public int getWeek(String eventTime)
	{
		Timestamp ts = Timestamp.valueOf(eventTime);
		Calendar cal = Calendar.getInstance();
		try
		{
			cal.setTime(new Date(ts.getTime()));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return cal.get(Calendar.WEEK_OF_YEAR);

	}
	

}
