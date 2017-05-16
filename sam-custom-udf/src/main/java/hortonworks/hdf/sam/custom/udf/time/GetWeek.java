package hortonworks.hdf.sam.custom.udf.time;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import com.hortonworks.streamline.streams.rule.UDF;

public class GetWeek implements UDF<Integer, String> {

	/**
	 * For a Given evenTime in String, return the week of the Year of for that eventTime
	 */
	public Integer evaluate(String eventTime) {
		Timestamp ts = Timestamp.valueOf(eventTime);
		Calendar cal = Calendar.getInstance();
		try
		{
			cal.setTime(new Date(ts.getTime()));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return cal.get(Calendar.WEEK_OF_YEAR);
	}

}
