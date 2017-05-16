package hortonworks.hdf.sam.custom.processor;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtils {

	
	@Test
	public void testSubstitute() {
		String sql = "select certified, wage_plan from drivers where driverid=${driverId}";
		Map<String, Object> valueMap = new HashMap<String, Object>();
		int driverId = 100;
		valueMap.put("driverId", driverId);
		StrSubstitutor substit = new StrSubstitutor(valueMap);
		
		String replacedValue = substit.replace(sql);
		
		Assert.assertEquals("select certified, wage_plan from drivers where driverid="+ driverId, replacedValue);
	}
	
	@Test
	public void outputField() {
		String enrichedOutputFields = "Certification, WagePlan";
		String trimmed = StringUtils.deleteWhitespace(enrichedOutputFields);
		String[] enrichFieldNames = trimmed.split(",");
		for (String value: enrichFieldNames) {
			System.out.println(value);
		}
	}
}
