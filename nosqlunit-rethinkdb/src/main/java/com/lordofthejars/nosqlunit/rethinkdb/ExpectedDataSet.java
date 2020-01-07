package com.lordofthejars.nosqlunit.rethinkdb;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Represents the expected RethinkDb dataset state, used for data comparison/assertions.
 * 
 * @author Affan Hasan
 */
public class ExpectedDataSet {

    public static final TypeReference<Map<String, List<Map<String, Object>>>> TYPE_REFERENCE = new TypeReference<Map<String, List<Map<String, Object>>>>() {
    };

    private final Map<String, List<Map<String, Object>>> dataset;

    /**
     * Default constructor.
     * 
     * @param dataset {@link Map<String, List<Map<String, Object>>>}
     */
    public ExpectedDataSet(final Map<String, List<Map<String, Object>>> dataset) {
        this.dataset = dataset;
    }

    /**
     * Returns the dataset.
     * 
     * @return {@link Map<String, List<Map<String, Object>>>}
     */
    public Map<String, List<Map<String, Object>>> getDataset() {
        return dataset;
    }

    /**
     * Returns database tables 
     * 
     * @return {@link Set<String>}
     */
    public Set<String> getTables() {
        return dataset.keySet();
    }

    /**
     * Returns data for a specific table.
     * 
     * @param tableName
     * @return {@link List<Map<String, Object>>}
     */
    public List<Map<String, Object>> getDataFor(final String tableName) {
        return dataset.get(tableName);
    }
}
