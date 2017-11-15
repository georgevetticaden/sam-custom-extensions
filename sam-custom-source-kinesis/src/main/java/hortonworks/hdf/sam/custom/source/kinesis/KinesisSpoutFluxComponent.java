package hortonworks.hdf.sam.custom.source.kinesis;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.shaded.services.kinesis.model.ShardIteratorType;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.component.StreamlineSource;
import com.hortonworks.streamline.streams.layout.storm.AbstractFluxComponent;
import com.hortonworks.streamline.streams.layout.storm.StormTopologyLayoutConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transformation Class that use SAM UI configs to configure the KinesisSpout
 * @author gvetticaden
 *
 */
public class KinesisSpoutFluxComponent extends AbstractFluxComponent {
	

	private static final Logger LOG = LoggerFactory.getLogger(KinesisSpoutFluxComponent.class);
	
	//prperties for KinesisConfig
    private static final String KEY_STREAM_NAME = "streamName";
    private static final String KEY_SHARD_ITERATOR_TYPE = "shardIteratorType";

    // properties for ExponentialBackoffRetrier
    private static final String KEY_RETRY_INITIAL_DELAY_MS = "retryInitialDelayMs";
    private static final String KEY_RETRY_BASE_SECONDS = "retryBaseSeconds";
    private static final String KEY_RETRY_MAX_TRIES = "retryMaxTries";
    
    // Zk related
    private static final String KEY_ZK_URL = "zkUrl";
    private static final String KEY_ZK_PATH = "zkPath";
    private static final String KEY_ZK_SESSION_TIMEOUT_MS = "zkSessionTimeoutMs";
    private static final String KEY_ZK_CONNECTION_TIMEOUT_MS = "zkConnectionTimeoutMs";
    private static final String KEY_ZK_COMMIT_INTERVAL_MS = "zkCommitIntervalMs";
    private static final String KEY_ZK_RETRY_ATTEMPTS = "zkRetryAttempts";
    private static final String KEY_ZK_RETRY_INTERVAL_MS = "zkRetryIntervalMs";


    private static final String KEY_MAX_UNCOMMITTED_RECORDS = "maxUncommittedRecords";
    
    //properties for AvroSRTupleMapper
    private static final String KEY_SCHEMA_REGISTRY_URL = "schemaRegistryUrl";
    private static final String KEY_READER_SCHEMA_VERSION = "readerSchemaVersion";

   //connection info related
	private static final Object KEY_AWS_KEY_ID = "awsAccessKeyId";
	private static final Object KEY_AWS_KEY_SECRET = "awsAccessKeySecret";
    private static final String KEY_AWS_REGION = "awsRegion";
    private static final String KEY_KINESIS_RECORDS_LIMIT = "kinesisRecordsLimit";	

	private String sourceId;

    @Override
    protected void generateComponent() {
    	
        StreamlineSource streamlineSource = (StreamlineSource) conf.get(StormTopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);
        // add the output stream to conf so that the kinesis spout declares output stream properly
        if (streamlineSource != null && streamlineSource.getOutputStreams().size() == 1) {
            conf.put(TopologyLayoutConstants.JSON_KEY_OUTPUT_STREAM_ID,
                    streamlineSource.getOutputStreams().iterator().next().getId());
        } else {
            String msg = "KinesisSpout source component [" + streamlineSource + "] should define exactly one output stream for Storm";
            LOG.error(msg, streamlineSource);
            throw new IllegalArgumentException(msg);
        }    	
    	
        this.sourceId = streamlineSource.getId();
        LOG.info("StreamLine Source Id is: " + this.sourceId);
        
        String spoutId = "kinesisSpout" + UUID_FOR_COMPONENTS;
        String spoutClassName = "org.apache.storm.kinesis.spout.KinesisSpout";
        
        String kinesisConfigRef = addKinesisConfigComponent();
        List<Object> constructorArgs = new ArrayList<>();
        addArg(constructorArgs, getRefYaml(kinesisConfigRef));
        
        List<Object> configMethods = new ArrayList<>();
        String[] configMethodNames = { "withOutputStream"};
        String[] configKeys = { TopologyLayoutConstants.JSON_KEY_OUTPUT_STREAM_ID};
        configMethods.addAll(getConfigMethodsYaml(configMethodNames, configKeys));          
        
        component = createComponent(spoutId, spoutClassName, null, constructorArgs, configMethods);
        addParallelismToComponent();
    }

    private String addKinesisConfigComponent() {
        String componentId = "kinesisConfig" + UUID_FOR_COMPONENTS;
        String className = "org.apache.storm.kinesis.spout.KinesisConfig";
        final List<Object> constructorArgs = new ArrayList<>();
        addArg(constructorArgs, KEY_STREAM_NAME);
        addArg(constructorArgs, KEY_SHARD_ITERATOR_TYPE);
        addArg(constructorArgs, getRefYaml(getRecordToTupleMapper()));
        addArg(constructorArgs, getRefYaml(getDate()));
        addArg(constructorArgs, getRefYaml(getFailedMessageRetryHandler()));
        addArg(constructorArgs, getRefYaml(getZkInfo()));
        addArg(constructorArgs, getRefYaml(getConnectionInfo()));
        addArg(constructorArgs, KEY_MAX_UNCOMMITTED_RECORDS, 10000);
        
        LOG.info("The constructors for org.apache.storm.kinesis.spout.KinesisConfig is: " + constructorArgs);
        
        addToComponents(createComponent(componentId, className, null, constructorArgs, null));
        return componentId;
    }

    private String getRecordToTupleMapper() {
        String componentId = "recordToTupleMapper" + UUID_FOR_COMPONENTS;
        String className = "hortonworks.hdf.sam.custom.source.kinesis.AvroSchemaRegistryRecordToTupleMapper";
        List<Object> constructorArgs = new ArrayList<>();
        addArg(constructorArgs, KEY_SCHEMA_REGISTRY_URL);
        addArg(constructorArgs, KEY_READER_SCHEMA_VERSION);
        constructorArgs.add(sourceId != null ? sourceId : "");
        
        LOG.info("The constructors for hortonworks.hdf.sam.custom.source.kinesis.AvroSchemaRegistryRecordToTupleMapper is: " + constructorArgs);
        
        addToComponents(createComponent(componentId, className, null, constructorArgs, null));
        return componentId;
    }

    private String getDate() {
        String componentId = "date" + UUID_FOR_COMPONENTS;
        String className = "java.util.Date";
        List<Object> constructorArgs = new ArrayList<>(); 
        /* Just add the data directly to arg and don't get it from Yaml fle */
        constructorArgs.add(System.currentTimeMillis());
        addToComponents(createComponent(componentId, className, null, constructorArgs, null));
        return componentId;
    }

    private String getFailedMessageRetryHandler() {
        String componentId = "retryHandler" + UUID_FOR_COMPONENTS;
        String className = "org.apache.storm.kinesis.spout.ExponentialBackoffRetrier";
        List<Object> constructorArgs = new ArrayList<>();
        addArg(constructorArgs, KEY_RETRY_INITIAL_DELAY_MS, 100);
        addArg(constructorArgs, KEY_RETRY_BASE_SECONDS, 2);
        addArg(constructorArgs, KEY_RETRY_MAX_TRIES, Long.MAX_VALUE);
        
        LOG.info("The constructors for org.apache.storm.kinesis.spout.ExponentialBackoffRetrier is: " + constructorArgs);
        
        addToComponents(createComponent(componentId, className, null, constructorArgs, null));
        return componentId;
    }

    private String getZkInfo() {
        String componentId = "zkInfo" + UUID_FOR_COMPONENTS;
        String className = "org.apache.storm.kinesis.spout.ZkInfo";
        List<Object> constructorArgs = new ArrayList<>();
        addArg(constructorArgs, KEY_ZK_URL);
        addArg(constructorArgs, KEY_ZK_PATH);
        addArg(constructorArgs, KEY_ZK_SESSION_TIMEOUT_MS, 20000);
        addArg(constructorArgs, KEY_ZK_CONNECTION_TIMEOUT_MS, 20000);
        addArg(constructorArgs, KEY_ZK_COMMIT_INTERVAL_MS, 10000);
        addArg(constructorArgs, KEY_ZK_RETRY_ATTEMPTS, 3);
        addArg(constructorArgs, KEY_ZK_RETRY_INTERVAL_MS, 2000);
                
        LOG.info("The constructors for org.apache.storm.kinesis.spout.ZkInfo is: " + constructorArgs);
        
        addToComponents(createComponent(componentId, className, null, constructorArgs, null));
        return componentId;
    }

    private String getConnectionInfo() {
        String componentId = "connectionInfo" + UUID_FOR_COMPONENTS;
        String className = "org.apache.storm.kinesis.spout.KinesisConnectionInfo";
        List<Object> constructorArgs = new ArrayList<>();
        addArg(constructorArgs, getRefYaml(getCredeentialsProviderChain()));
        addArg(constructorArgs, getRefYaml(getClientConfiguration()));
        addArg(constructorArgs, KEY_AWS_REGION);
        addArg(constructorArgs, KEY_KINESIS_RECORDS_LIMIT, 1000);
        addToComponents(createComponent(componentId, className, null, constructorArgs, null));
        
        LOG.info("The constructors for org.apache.storm.kinesis.spout.KinesisConnectionInfo is: " + constructorArgs);
        
        return componentId;
    }

    private String getCredeentialsProviderChain() {
    	
    	String componentId = "credentialsProviderChain" + UUID_FOR_COMPONENTS;
    	String className = "com.amazonaws.shaded.auth.AWSStaticCredentialsProvider";
    	
    	List<Object> constructorArgs = new ArrayList<>();
    	addArg(constructorArgs, getRefYaml(getBasicAWSCredentials()));
    	
        LOG.info("The constructors for com.amazonaws.auth.AWSStaticCredentialsProvider is: " + constructorArgs);
        
        addToComponents(createComponent(componentId, className, null, constructorArgs, null));
        return componentId;
    }

    private String getBasicAWSCredentials() {
    	String componentId = "basicAWSCredentials" + UUID_FOR_COMPONENTS;
    	String className = "com.amazonaws.shaded.auth.BasicAWSCredentials";
    	List<Object> constructorArgs = new ArrayList<>();
    	addArg(constructorArgs, KEY_AWS_KEY_ID);
    	addArg(constructorArgs, KEY_AWS_KEY_SECRET);
    	
        LOG.info("The constructors for com.amazonaws.auth.BasicAWSCredentials is: " + constructorArgs);

    	addToComponents(createComponent(componentId, className, null, constructorArgs, null));
    	
    	return componentId;
	}

	private String getClientConfiguration() {
        String componentId = "clientConfiguration" + UUID_FOR_COMPONENTS;
        String className = "com.amazonaws.shaded.ClientConfiguration";
        addToComponents(createComponent(componentId, className, null, null, null));
        return componentId;
    }

}