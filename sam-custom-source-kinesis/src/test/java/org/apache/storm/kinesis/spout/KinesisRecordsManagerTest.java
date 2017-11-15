package org.apache.storm.kinesis.spout;

import hortonworks.hdf.sam.custom.source.kinesis.AvroSchemaRegistryRecordToTupleMapper;

import java.util.Date;

import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

public class KinesisRecordsManagerTest {

	protected static final Logger LOG = LoggerFactory
			.getLogger(KinesisRecordsManagerTest.class);	
	
	
	private static final String ZK_URL = "hdf-3-1-build0.field.hortonworks.com:2181";
	private static final String KINESIS_TRUCK_STREAM_NAME = "truck_events_avro";
	private static final String KINESIS_TRUCK_SPEED_STREAM_NAME = "truck_speed_events_avro";
	private static final Regions AWS_REGION = Regions.US_WEST_2;
	private static final String SCHEMA_REGISTRY_TRUCK_GEO_EVENT_READER_SCHEMA_READER_VERSION = "1";
	private static final String SCHEMA_REGISTRY_URL = "http://hdf-3-1-build3.field.hortonworks.com:7788/api/v1";
	
	private static final long SPOUT_MAX_UNCOMMITTED_RECORDS = 10000L;
	private static final int ZK_RETRY_INTERVAL = 2000;
	private static final int ZK_RETRY_ATTEMPTS = 3;
	private static final long ZK_COMMIT_INTERVAL = 10000L;
	private static final int ZK_CONNECTION_TIME_OUT = 15000;
	private static final int ZK_SESSION_TIME_OUT = 20000;
	private static final String ZK_NODE_KINESIS_OFFSETS = "/kinesisOffsets";
	private static final Integer KINESIS_MAX_RECORDS = 1000;
	
	
	@Test
	public void createRegion() {
		Regions regions  = Regions.valueOf("US_EAST_2");
		Assert.assertNotNull(regions);
		LOG.info(regions.getName());
	}
	


	@Test
	public void tesKinesisRecordManagerforTruckGeoEvent() {
		
		LOG.info("about to execute test: tesKinesisRecordManagerforTruckGeoEvent");
		
		KinesisConnectionInfo kinesisConnectionInfo = createKinesisConnectionInfo(new DefaultAWSCredentialsProviderChain(), 
				 																  new ClientConfiguration(), 
				 																  AWS_REGION, 
																				  KINESIS_MAX_RECORDS);
		
	
		
		String dataSourceId = "1";
		RecordToTupleMapper tupleMapper = createSchemaRegistryAvroTupleMapper(SCHEMA_REGISTRY_URL, 
																			  SCHEMA_REGISTRY_TRUCK_GEO_EVENT_READER_SCHEMA_READER_VERSION, 
																			  dataSourceId);
		
		KinesisConfig kinesisConfig = createKinesisConfig(ZK_URL, KINESIS_TRUCK_STREAM_NAME, tupleMapper, kinesisConnectionInfo);
		KinesisRecordsManager kinesisRecordsManager = new KinesisRecordsManager(kinesisConfig, "outputStream");
		kinesisRecordsManager.initialize(1, 1);
//		SpoutOutputCollector collector = null;
//		LOG.info("about to call next...");
//		kinesisRecordsManager.next(collector);
//		LOG.info("Done calling next...");
	}


	private KinesisConfig createKinesisConfig(String zkUrl, String streamName,  RecordToTupleMapper avroTupleMapper, KinesisConnectionInfo kinesisConnectionInfo ) {
        ZkInfo zkInfo = new ZkInfo(zkUrl, ZK_NODE_KINESIS_OFFSETS, ZK_SESSION_TIME_OUT, ZK_CONNECTION_TIME_OUT, ZK_COMMIT_INTERVAL, ZK_RETRY_ATTEMPTS, ZK_RETRY_INTERVAL);
        
		KinesisConfig kinesisConfig = new KinesisConfig(streamName, ShardIteratorType.TRIM_HORIZON.name(),
				avroTupleMapper, new Date(), new ExponentialBackoffRetrier(), zkInfo, kinesisConnectionInfo, SPOUT_MAX_UNCOMMITTED_RECORDS);
		
		return kinesisConfig;
	}

	private RecordToTupleMapper createSchemaRegistryAvroTupleMapper(String schemaRegistryUrl, String readerSchemaVersion, String dataSourceId) {
		AvroSchemaRegistryRecordToTupleMapper mapper = new AvroSchemaRegistryRecordToTupleMapper(schemaRegistryUrl, readerSchemaVersion, dataSourceId);
		return mapper;
	}
	
	private KinesisConnectionInfo createKinesisConnectionInfo(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration, Regions region, Integer recordsLimit) {
		KinesisConnectionInfo kinesisConnectionInfo = new KinesisConnectionInfo(credentialsProvider, clientConfiguration, region.name(), recordsLimit);
		return kinesisConnectionInfo;
	}
	
	
}
