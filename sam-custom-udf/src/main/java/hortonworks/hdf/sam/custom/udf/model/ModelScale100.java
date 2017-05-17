package hortonworks.hdf.sam.custom.udf.model;

import com.hortonworks.streamline.streams.rule.UDF;

public class ModelScale100 implements UDF<Double, String> {

	@Override
	public Double evaluate(String value) {
		Double valueDouble = Double.valueOf(value);
		return valueDouble/100;
	}

}
