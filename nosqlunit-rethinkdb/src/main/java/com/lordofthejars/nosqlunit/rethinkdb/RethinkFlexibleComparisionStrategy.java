package com.lordofthejars.nosqlunit.rethinkdb;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lordofthejars.nosqlunit.core.ComparisonStrategy;

/**
 * Core class represents a flexible comparision strategy.
 * 
 * @author Affan Hasan
 */
public class RethinkFlexibleComparisionStrategy implements ComparisonStrategy<RethinkDbConnectionCallback> {

    private String[] ignorePropertyValues = new String[0];

    /* (non-Javadoc)
     * @see com.lordofthejars.nosqlunit.core.ComparisonStrategy#compare(java.lang.Object, java.io.InputStream)
     */
    @Override
    public boolean compare(RethinkDbConnectionCallback connectionCallback, InputStream dataset) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

        final Map<String, List<Map<String, Object>>> parsedData = objectMapper.readValue(dataset,
                ExpectedDataSet.TYPE_REFERENCE);

        RethinkDbAssertion.flexibleAssertEquals(new ExpectedDataSet(parsedData), ignorePropertyValues, connectionCallback);
        return true;
    }

    /* (non-Javadoc)
     * @see com.lordofthejars.nosqlunit.core.ComparisonStrategy#setIgnoreProperties(java.lang.String[])
     */
    @Override
    public void setIgnoreProperties(final String[] ignorePropertyValues) {
        this.ignorePropertyValues = ignorePropertyValues;
    }
}
