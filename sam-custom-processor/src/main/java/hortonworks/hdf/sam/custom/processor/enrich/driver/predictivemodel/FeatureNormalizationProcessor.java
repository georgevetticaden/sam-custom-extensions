package hortonworks.hdf.sam.custom.processor.enrich.driver.predictivemodel;

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
 * Normalizes features that will be fed into the Predictive model. Normalization includes:
 * 	1. Feature scaling applied to hours and miles to improve algorithm performance
 *  2. Normalize the features of the Drivers Wage Plan and Certification to binary values
 * @author gvetticaden
 *
 */
public class FeatureNormalizationProcessor implements CustomProcessorRuntime {


	private static final String CONFIG_NORMALIZE = "normalize";	
	
	private static final String INPUT_DRIVER_CERT_KEY = "driverCertification";
	private static final String INPUT_DRIVER_WAGE_PLAN_KEY = "driverWagePlan";
	private static final String INPUT_DRIVER_HOURS_KEY = "driverFatigueByHours";
	private static final String INPUT_DRIVER_MILES_KEY = "driverFatigueByMiles";
	
	private static final String OUTPUT_DRIVER_CERT_NORMALIZED_KEY = "Model_Feature_Certification";
	private static final String OUTPUT_DRIVER_WAGE_PLAN_NORMALIZED_KEY = "Model_Feature_WagePlan";
	private static final String OUTPUT_DRIVER_HOURS_NORMALIZED_KEY = "Model_Feature_FatigueByHours";
	private static final String OUTPUT_DRIVER_MILES_NORMALIZED_KEY = "Model_Feature_FatigueByMiles";
	
	
	protected static final Logger LOG = LoggerFactory.getLogger(FeatureNormalizationProcessor.class);


	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void initialize(Map<String, Object> config) {
		LOG.info("Initialzing FeatureNormalization processor");
		
	}

	@Override
	public List<StreamlineEvent> process(StreamlineEvent event)
			throws ProcessingException {
		
		LOG.info("About to do feature normalization event: " + event);
		
		
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
        builder.putAll(event);
        
        Map<String, Object> normalizedFeatures = new HashMap<String, Object>();
        
        String driverCert = (String) event.get(INPUT_DRIVER_CERT_KEY);
        normalizedFeatures.put(OUTPUT_DRIVER_CERT_NORMALIZED_KEY, normalizeDriverCert(driverCert));
        
        String driverWagePlan = (String)event.get(INPUT_DRIVER_WAGE_PLAN_KEY);
        normalizedFeatures.put(OUTPUT_DRIVER_WAGE_PLAN_NORMALIZED_KEY, normalizedDriverWagePlan(driverWagePlan));
        
        String driverHours = (String) event.get(INPUT_DRIVER_HOURS_KEY);
        normalizedFeatures.put(OUTPUT_DRIVER_HOURS_NORMALIZED_KEY, normalizeDriverHours(driverHours));
        
        String driverMiles = (String) event.get(INPUT_DRIVER_MILES_KEY);
        normalizedFeatures.put(OUTPUT_DRIVER_MILES_NORMALIZED_KEY, normalizeDriverMiles(driverMiles));
        
        LOG.info("Normalized Feautres are: " + normalizedFeatures);
		
        //add the new normalized feautres to the builder
        builder.putAll(normalizedFeatures);

      
        List<Result> results = new ArrayList<Result>();
        
        //create new event
        StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
        LOG.info("Enriched StreamLine Event with normalization  is: " + enrichedEvent );
        
        List<StreamlineEvent> newEvents= Collections.<StreamlineEvent>singletonList(enrichedEvent);
        return newEvents;  
	}

	private Double normalizeDriverMiles(String driverMiles) {
		//scale for spark model
		Double driverMilesDouble = Double.valueOf(driverMiles);
		return driverMilesDouble/1000;
	}

	private Double normalizeDriverHours(String driverHours) {
		//scale for spark model
		Double driverHourseDouble = Double.valueOf(driverHours);
		return driverHourseDouble/100;		
	}

	private Integer normalizedDriverWagePlan(String driverWagePlan) {
		return driverWagePlan.equals("miles") ? 1 : 0;
	}

	private Integer normalizeDriverCert(String driverCert) {
		return driverCert.equals("Y") ? 1 : 0;
	}

	@Override
	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		// TODO Auto-generated method stub

	}


}
