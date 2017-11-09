package hortonworks.hdf.sam.custom.processor.enrich.weather;

import java.util.ArrayList;
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
 * Enriches the event with fabricated weather conditions (fog, rain and wind)
 * These conditions are then normalized into binary values based on the intensity of the weather codition
 * @author gvetticaden
 *
 */
public class WeatherEnrichmentProcessor implements CustomProcessorRuntime {


	private static final String CONFIG_WEATHER_SERVICE_URL = "weatherServiceURL";
	
	private static final String INPUT_DRIVER_ID_KEY = "driverId";
	
	private static final String OUTPUT_FOGGY_WEATHER_ENRICH_KEY="Model_Feature_FoggyWeather";
	private static final String OUTPUT_RAINY_WEATHER_ENRICH_KEY="Model_Feature_RainyWeather";
	private static final String OUTPUT_WINDY_WEATHER_ENRICH_KEY="Model_Feature_WindyWeather";
	
	protected static final Logger LOG = LoggerFactory.getLogger(WeatherEnrichmentProcessor.class);

		
	


	private String  weatherServiceUrl;	
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void initialize(Map<String, Object> config) {
		LOG.info("Initialzing Weather Processor");
		
		this.weatherServiceUrl = (String)config.get(CONFIG_WEATHER_SERVICE_URL);
		
		LOG.info("The weather Service URL configured is: " + weatherServiceUrl);

	}

	@Override
	public List<StreamlineEvent> process(StreamlineEvent event)
			throws ProcessingException {
		Integer driverId = (Integer) event.get(INPUT_DRIVER_ID_KEY);
		LOG.info("Driver["+driverId+"] about to be enriched with weather data");
		
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
        builder.putAll(event);
        
		if(driverId != null) {
			
			Map<String, Object> enrichedWithWeather = enrich(driverId);
			LOG.info("Enriching driver["+driverId + "]  with the following enriched weather values: " + enrichedWithWeather);
			builder.putAll(enrichedWithWeather);
			
		} else  {
			LOG.info("Skipping Enrichment because driverId was null..");
		}
        
        StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
        LOG.info("Enriched StreamLine Event with weather is: " + enrichedEvent );
        List<StreamlineEvent> newEvents= Collections.<StreamlineEvent>singletonList(enrichedEvent);
        return newEvents; 
	}

	@Override
	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		// TODO Auto-generated method stub

	}
	
	private Map<String, Object> enrich(Integer driverId) {
		Map<String, Object> enrichedValues = new HashMap<String, Object>();
		enrichedValues.put(OUTPUT_FOGGY_WEATHER_ENRICH_KEY, getFoggyWeatherConditions(driverId));
		enrichedValues.put(OUTPUT_RAINY_WEATHER_ENRICH_KEY, getRainyWeatherConditions());
		enrichedValues.put(OUTPUT_WINDY_WEATHER_ENRICH_KEY, getWindyWeatherConditions());
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

}
