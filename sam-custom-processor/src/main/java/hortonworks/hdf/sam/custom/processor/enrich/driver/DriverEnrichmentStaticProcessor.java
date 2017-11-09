package hortonworks.hdf.sam.custom.processor.enrich.driver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.common.exception.ConfigException;
import com.hortonworks.streamline.streams.Result;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

/**
 * A mocked out static enrichment processor for Drivers for Testing
 * @author gvetticaden
 *
 */
public class DriverEnrichmentStaticProcessor implements CustomProcessorRuntime {

	protected static final Logger LOG = LoggerFactory.getLogger(DriverEnrichmentStaticProcessor.class);
	private static final String DRIVER_ID_KEY = "driverId";
	private static final String CERTIFICATION_ENRICH_KEY="Certification";
	private static final String WAGE_PLAN_ENRICH_KEY="WagePlan";
	private static final String FATIGUE_BY_HOURS_ENRICH_KEY="FatigueByHours";
	private static final String FATIGUE_BY_MILES_ENRICH_KEY="FatigueByMiles";
	private static final String FOGGY_WEATHER_ENRICH_KEY="FoggyWeather";
	private static final String RAINY_WEATHER_ENRICH_KEY="RainyWeather";
	private static final String WINDY_WEATHER_ENRICH_KEY="WindyWeather";
	
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void initialize(Map<String, Object> args) {
		LOG.info("Initializing + " + DriverEnrichmentStaticProcessor.class.getName());
		
	}

	public List<StreamlineEvent> process(StreamlineEvent event)
			throws ProcessingException {
		Integer driverId = (Integer) event.get(DRIVER_ID_KEY);
		LOG.info("Driver["+driverId+"] about to be enriched");
		
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
        builder.putAll(event);
        
		if(driverId != null) {
			Map<String, Object> enrichedDriverInfo = enrich(driverId);
			LOG.info("Enriching driver["+driverId + "]  with the following enriched values: " + enrichedDriverInfo);
			//event.addFieldsAndValues(enrichedDriverInfo);
			builder.putAll(enrichedDriverInfo);
		} else  {
			LOG.info("Skipping Enrichment because driverId was null..");
		}

			
        List<Result> results = new ArrayList<Result>();
        StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
        LOG.info("Enriched StreamLine Event is: " + enrichedEvent );
        List<StreamlineEvent> newEvents= Collections.<StreamlineEvent>singletonList(enrichedEvent);
        return newEvents;
	}

	private Map<String, Object> enrich(Integer driverId) {
		Map<String, Object> enrichedValues = new HashMap<String, Object>();
		enrichedValues.put(CERTIFICATION_ENRICH_KEY, getDriverCertification());
		enrichedValues.put(WAGE_PLAN_ENRICH_KEY, getDriverWagePlan());
		enrichedValues.put(FATIGUE_BY_HOURS_ENRICH_KEY, getDriverLoggedHours());
		enrichedValues.put(FATIGUE_BY_MILES_ENRICH_KEY,getDriverLoggedMiles());
		enrichedValues.put(FOGGY_WEATHER_ENRICH_KEY, getFoggyWeatherConditions(driverId));
		enrichedValues.put(RAINY_WEATHER_ENRICH_KEY, getRainyWeatherConditions());
		enrichedValues.put(WINDY_WEATHER_ENRICH_KEY, getWindyWeatherConditions());
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

	private Object getDriverLoggedMiles() {
		double milesLogged = 3300;
		double scaledMilesLogged = milesLogged/1000;
		return scaledMilesLogged;
	}

	private Object getDriverLoggedHours() {
		double hoursLogged = 80;
		double scaledHours = hoursLogged/100;
		return scaledHours;
	}

	private Object getDriverWagePlan() {
		return Double.valueOf(0);
	}

	private Object getDriverCertification() {
		return Double.valueOf(0);
	}

	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		// TODO Auto-generated method stub
		
	}

}
