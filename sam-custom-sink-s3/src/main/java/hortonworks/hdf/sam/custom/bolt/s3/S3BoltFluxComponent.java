package hortonworks.hdf.sam.custom.bolt.s3;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.layout.storm.AbstractFluxComponent;

public class S3BoltFluxComponent extends AbstractFluxComponent {

	private static final Logger LOG = LoggerFactory.getLogger(S3BoltFluxComponent.class);
	
	// connection info related
	private static final Object KEY_AWS_KEY_ID = "awsAccessKeyId";
	private static final Object KEY_AWS_KEY_SECRET = "awsAccessKeySecret";
	private static final String KEY_AWS_REGION = "awsRegion";
	private static final String KEY_BUCKET_NAME = "s3Bucket";

	@Override
	protected void generateComponent() {

		String boltId = "s3Bolt" + UUID_FOR_COMPONENTS;
	    String boltClassName = "hortonworks.storm.aws.s3.bolt.S3Bolt";
	    
	    final List<Object> constructorArgs = new ArrayList<>();
	    addArg(constructorArgs, getRefYaml(getConnectionInfo()));
	    addArg(constructorArgs, KEY_BUCKET_NAME);
	        	        
	    component = createComponent(boltId, boltClassName, null, constructorArgs, null);
	    addParallelismToComponent();

	}

	private String getConnectionInfo() {
		String componentId = "s3connectionInfo" + UUID_FOR_COMPONENTS;
		String className = "hortonworks.storm.aws.s3.S3ConnectionInfo";
		List<Object> constructorArgs = new ArrayList<>();
		addArg(constructorArgs, getRefYaml(getCredentialsProviderChain()));
		addArg(constructorArgs, getRefYaml(getClientConfiguration()));
		addArg(constructorArgs, KEY_AWS_REGION);
		addToComponents(createComponent(componentId, className, null,
				constructorArgs, null));

		LOG.info("The constructors for hortonworks.storm.aws.s3.S3ConnectionInfo is: "
				+ constructorArgs);

		return componentId;
	}

	private String getCredentialsProviderChain() {

		String componentId = "s3CredentialsProviderChain" + UUID_FOR_COMPONENTS;
		String className = "com.amazonaws.shaded.auth.AWSStaticCredentialsProvider";

		List<Object> constructorArgs = new ArrayList<>();
		addArg(constructorArgs, getRefYaml(getBasicAWSCredentials()));

		LOG.info("The constructors for com.amazonaws.auth.AWSStaticCredentialsProvider is: "
				+ constructorArgs);

		addToComponents(createComponent(componentId, className, null,
				constructorArgs, null));
		return componentId;
	}

	private String getBasicAWSCredentials() {
		String componentId = "basicAWSCredentials" + UUID_FOR_COMPONENTS;
		String className = "com.amazonaws.shaded.auth.BasicAWSCredentials";
		List<Object> constructorArgs = new ArrayList<>();
		addArg(constructorArgs, KEY_AWS_KEY_ID);
		addArg(constructorArgs, KEY_AWS_KEY_SECRET);

		LOG.info("The constructors for com.amazonaws.auth.BasicAWSCredentials is: "
				+ constructorArgs);

		addToComponents(createComponent(componentId, className, null,
				constructorArgs, null));

		return componentId;
	}

	private String getClientConfiguration() {
		String componentId = "s3ClientConfiguration" + UUID_FOR_COMPONENTS;
		String className = "com.amazonaws.shaded.ClientConfiguration";
		addToComponents(createComponent(componentId, className, null, null,
				null));
		return componentId;
	}

}
