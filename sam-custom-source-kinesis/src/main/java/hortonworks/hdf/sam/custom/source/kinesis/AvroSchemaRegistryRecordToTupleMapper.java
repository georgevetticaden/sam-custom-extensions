package hortonworks.hdf.sam.custom.source.kinesis;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.kinesis.spout.RecordToTupleMapper;
import org.apache.storm.tuple.Fields;

import com.amazonaws.shaded.services.kinesis.model.Record;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.runtime.storm.spout.AvroKafkaSpoutTranslator;
import com.hortonworks.streamline.streams.runtime.storm.spout.AvroStreamsSnapshotDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Maps a Kinesis Record containing a payload serialized by the SR AvroSerializer and deserializes using SR's deserarilizer into a StreamLineEvent
 * @author gvetticaden
 *
 */
public class AvroSchemaRegistryRecordToTupleMapper implements
		RecordToTupleMapper, Serializable {


	private static final long serialVersionUID = -48303611622012569L;
	private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaRegistryRecordToTupleMapper.class);
	
	private String schemaRegistryUrl;
	private Integer readerSchemaVersion;
	private String dataSourceId;	
	
	private transient volatile AvroStreamsSnapshotDeserializer avroStreamsSnapshotDeserializer;


	public AvroSchemaRegistryRecordToTupleMapper(String schemaRegistryUrl,
			String schemaVersion, String dataSourceId) {
		super();
		this.schemaRegistryUrl = schemaRegistryUrl;
		this.readerSchemaVersion = Integer.valueOf(schemaVersion);
		this.dataSourceId = dataSourceId;		
	}



	@Override
	public List<Object> getTuple(Record record) {

		
        Map<String, Object> keyValues = (Map<String, Object>) getDeserializer().deserialize(new AvroKafkaSpoutTranslator.ByteBufferInputStream(record.getData()),
                readerSchemaVersion);		
        
        StreamlineEvent streamlineEvent = StreamlineEventImpl.builder().putAll(keyValues).dataSourceId(dataSourceId).build();
        
        LOG.debug("Deserialized Kinesis Record["+record + "] to StreamLineEvent[" + streamlineEvent);
        
        return Collections.<Object> singletonList(streamlineEvent);	
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(StreamlineEvent.STREAMLINE_EVENT);
	}
	
    private AvroStreamsSnapshotDeserializer getDeserializer () {
    	if(avroStreamsSnapshotDeserializer == null) {
    		avroStreamsSnapshotDeserializer = new AvroStreamsSnapshotDeserializer();
    		Map<String, Object> config = new HashMap<>();
            config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
            avroStreamsSnapshotDeserializer.init(config);
            LOG.info("AvroSchemaRegistryRecordToTupleMapper created successfully");
    	}
    	return avroStreamsSnapshotDeserializer;
    }	

}
