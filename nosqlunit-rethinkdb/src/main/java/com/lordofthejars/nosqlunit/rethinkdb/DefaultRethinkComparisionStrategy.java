package com.lordofthejars.nosqlunit.rethinkdb;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lordofthejars.nosqlunit.core.ComparisonStrategy;

/**
 * Default implementation of {@link ComparisonStrategy}.
 * 
 * @author Affan Hasan
 */
public class DefaultRethinkComparisionStrategy implements ComparisonStrategy<RethinkDbConnectionCallback> {

	/* (non-Javadoc)
	 * @see com.lordofthejars.nosqlunit.core.ComparisonStrategy#compare(java.lang.Object, java.io.InputStream)
	 */
	@Override
    public boolean compare(final RethinkDbConnectionCallback rethinkDbConnectionCallback, final InputStream dataset)
    		throws Throwable {
		final ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
		
		final Map<String, List<Map<String, Object>>> parsedData = objectMapper.readValue(dataset, ExpectedDataSet.TYPE_REFERENCE);
		final ExpectedDataSet expectedDataset = new ExpectedDataSet(parsedData);
		RethinkDbAssertion.strictAssertEquals(expectedDataset, rethinkDbConnectionCallback);
		return true;
	}

	@Override
	public void setIgnoreProperties(final String[] ignoreProperties) {
		// Implementation not required
	}
}
