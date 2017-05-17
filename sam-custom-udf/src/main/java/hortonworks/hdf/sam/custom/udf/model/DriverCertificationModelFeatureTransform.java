package hortonworks.hdf.sam.custom.udf.model;

import com.hortonworks.streamline.streams.rule.UDF;

public class DriverCertificationModelFeatureTransform implements UDF<Integer, String> {

	@Override
	public Integer evaluate(String value) {
		Integer result = value.equals("Y") ? 1 : 0;
		return result;
	}

}
