package hortonworks.hdf.sam.custom.udf.math;

import com.hortonworks.streamline.streams.rule.UDF;

public class Round implements UDF<Long, Double> {

	public Long evaluate(Double value) {
		return Math.round(value);
	}

}
