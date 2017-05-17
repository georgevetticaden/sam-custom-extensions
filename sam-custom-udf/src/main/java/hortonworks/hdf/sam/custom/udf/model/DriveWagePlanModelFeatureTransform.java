package hortonworks.hdf.sam.custom.udf.model;

import com.hortonworks.streamline.streams.rule.UDF;

public class DriveWagePlanModelFeatureTransform implements UDF<Integer, String> {

	@Override
	public Integer evaluate(String value) {
		Integer result = value.equals("miles") ? 1 : 0;
		return result;
	}

}
