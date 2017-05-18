package hortonworks.hdf.sam.custom.udf.time;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import com.hortonworks.streamline.streams.rule.UDF;

public class ConvertToTimestampLong implements UDF<Long, String> {

	/**
	 * For a Given evenTime in String, return the timestamp long value
	 */
	public Long evaluate(String eventTime) {
		Timestamp ts = Timestamp.valueOf(eventTime);
		return ts.getTime();
	}

}
