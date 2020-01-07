package com.lordofthejars.nosqlunit.rethinkdb;

import static com.rethinkdb.RethinkDB.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.rethinkdb.net.Cursor;

/**
 * Contains static methods for containing low level assertion logic for strict and flexible assertion strategies.
 * 
 * @author Affan Hasan
 */
public class RethinkDbAssertion {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RethinkDbAssertion.class);
	
	private RethinkDbAssertion() {
		super();
	}
	
    /**
     * Contains comparison logic for strict assertion strategy.
     * 
     * @param expectedData {@link ExpectedDataSet}
     * @param rethinkDbConnectionCallback {@link RethinkDbConnectionCallback}
     */
    public static final void strictAssertEquals(final ExpectedDataSet expectedData, final RethinkDbConnectionCallback rethinkDbConnectionCallback) {
        final Set<String> expectedTableNames = expectedData.getTables();
        final List<String> tableNames = r.tableList() //
        		.run(rethinkDbConnectionCallback.dbConnection());
        
        checkTablesName(expectedTableNames, tableNames);
        for (final String tableName : expectedTableNames) {
            checkTableObjects(expectedData, rethinkDbConnectionCallback, tableName);
        }
    }
    
    /**
     * Contains flexible comparison logic for flexible comparison strategy
     * 
     * @param expectedData {@link ExpectedDataSet}
     * @param ignorePropertyValues {@link String[]}
     * @param rethinkDbConnectionCallback {@link RethinkDbConnectionCallback}
     */
    public static void flexibleAssertEquals(final ExpectedDataSet expectedData, final String[] ignorePropertyValues,
            final RethinkDbConnectionCallback rethinkDbConnectionCallback) {
        // Get the expected tables
        final Set<String> tableNames = expectedData.getTables();

        // Get the current tables
        final List<String> listTableNames = r.tableList() //
        		.run(rethinkDbConnectionCallback.dbConnection());

        // Get the concrete property names that should be ignored
        final Map<String, Set<String>> propertiesToIgnore = parseIgnorePropertyValues(tableNames, ignorePropertyValues);

        // Check expected data
        flexibleCheckTablesName(tableNames, listTableNames);
        for (final String tableName : tableNames) {
            flexibleCheckTableObjects(expectedData, rethinkDbConnectionCallback, tableName, propertiesToIgnore);
        }
    }
    
    private static void flexibleCheckTablesName(final Set<String> expectedTableNames,
            final List<String> dynamodbTableNames) {
        boolean ok = true;
        final HashSet<String> notFoundTableNames = new HashSet<>();
        for (final String expectedTableName : expectedTableNames) {
            if (!dynamodbTableNames.contains(expectedTableName)) {
                ok = false;
                notFoundTableNames.add(expectedTableName);
            }
        }

        if (!ok) {
            throw FailureHandler.createFailure(
                    "The following table names %s were not found in the inserted table names", notFoundTableNames);
        }
    }
    
    private static void checkTableObjects(final ExpectedDataSet expectedData, final RethinkDbConnectionCallback rethinkDbConnectionCallback,
            final String tableName) {
        final List<Map<String, Object>> dataObjects = expectedData.getDataFor(tableName);
        final Cursor<Map<String, Object>> cursor = r.table(tableName) //
        .run(rethinkDbConnectionCallback.dbConnection());
        final List<Map<String, Object>> dbTable = cursor.toList();
        
        final int expectedDataObjectsCount = dataObjects.size();
        final long insertedDataObjectsCount = dbTable.size();

        if (expectedDataObjectsCount != insertedDataObjectsCount) {
            throw FailureHandler.createFailure("Expected table has %s elements but insert table has %s",
                    expectedDataObjectsCount, insertedDataObjectsCount);
        }

        for (final Map<String, Object> expectedDataObject : dataObjects) {
            dbTable.stream() //
                    .filter(map -> map.equals(expectedDataObject)) //
                    .findFirst() //
                    .orElseThrow(() -> FailureHandler.createFailure("Object # %s # is not found into table [%s]",
                            expectedDataObject.toString(), tableName));
        }
    }
    
    private static void flexibleCheckTableObjects(final ExpectedDataSet expectedData, final RethinkDbConnectionCallback rethinkDbConnectionCallback,
            final String tableName, final Map<String, Set<String>> propertiesToIgnore) {
        final List<Map<String, Object>> dataObjects = expectedData.getDataFor(tableName);
        final Cursor<Map<String, Object>> cursor = r.table(tableName) //
                .run(rethinkDbConnectionCallback.dbConnection());

        final List<Map<String, Object>> dbTable = cursor.toList();

        for (final Map<String, Object> expectedDataObject : dataObjects) {
            final Map<String, Object> filteredExpectedDataObject = filterProperties(expectedDataObject,
                    propertiesToIgnore.get(tableName));
            final List<Map<String, Object>> foundObjects = dbTable.stream() //
                    .map(foundDataObject -> filterProperties(foundDataObject, propertiesToIgnore.get(tableName))) //
                    .filter(map -> map.equals(filteredExpectedDataObject)) //
                    .collect(Collectors.toList());

            if (foundObjects.size() > 1) {
                LOGGER.warn(
                        "There were found {} possible matches for this object # {} #. That could have been caused by ignoring too many properties.",
                        foundObjects.size(), expectedDataObject);
            }

            if (foundObjects.isEmpty()) {
                throw FailureHandler.createFailure("Object # %s # is not found into table [%s]",
                        filteredExpectedDataObject.toString(), tableName);
            }

        }
    }
    
    private static Map<String, Object> filterProperties(final Map<String, Object> dataObject,
            final Set<String> propertiesToIgnore) {
        final Map<String, Object> filteredDataObject = new HashMap<>();

        for (final Map.Entry<String, Object> entry : dataObject.entrySet()) {
            if (propertiesToIgnore == null || !propertiesToIgnore.contains(entry.getKey())) {
                filteredDataObject.put(entry.getKey(), entry.getValue());
            }
        }

        return filteredDataObject;
    }
    
    /**
     * Resolve the properties that will be ignored for each expected table.
     * <p/>
     *
     * @param ignorePropertyValues Input values defined with @IgnorePropertyValue.
     * @return Map with the properties that will be ignored for each document.
     */
    private static Map<String, Set<String>> parseIgnorePropertyValues(final Set<String> tableNames,
            final String[] ignorePropertyValues) {
        final Map<String, Set<String>> propertiesToIgnore = new HashMap<>();
        final Pattern tableAndPropertyPattern = Pattern.compile(
                "^(?!system\\.)([a-z,A-Z,_][^$\0]*)([.])([^$][^.\0]*)$");
        final Pattern propertyPattern = Pattern.compile("^([^$][^.0]*)$");

        for (final String ignorePropertyValue : ignorePropertyValues) {
            final Matcher tableAndPropertyMatcher = tableAndPropertyPattern.matcher(ignorePropertyValue);
            final Matcher propertyMatcher = propertyPattern.matcher(ignorePropertyValue);

            // If the property to ignore includes the table, add it to only exclude
            // the property in the indicated table
            if (tableAndPropertyMatcher.matches()) {
                // Add the property to ignore to the proper table
                final String tableName = tableAndPropertyMatcher.group(1);
                final String propertyName = tableAndPropertyMatcher.group(3);

                if (tableNames.contains(tableName)) {
                    Set<String> properties = propertiesToIgnore.get(tableName);
                    if (properties == null) {
                        properties = new HashSet<>();
                    }
                    properties.add(propertyName);
                    propertiesToIgnore.put(tableName, properties);
                } else {
                    LOGGER.warn("Table {} for {} is not defined as expected. It won't be used for ignoring properties",
                            tableName, ignorePropertyValue);
                }
                // If the property to ignore doesn't include the table, add it to
                // all the expected tables
            } else if (propertyMatcher.matches()) {
                final String propertyName = propertyMatcher.group(0);

                // Add the property to ignore to all the expected tables
                for (final String tableName : tableNames) {
                    Set<String> properties = propertiesToIgnore.get(tableName);
                    if (properties == null) {
                        properties = new HashSet<>();
                    }
                    properties.add(propertyName);
                    propertiesToIgnore.put(tableName, properties);
                }
                // If doesn't match any pattern
            } else {
                LOGGER.warn("Property {} has an invalid table.property value. It won't be used for ignoring properties",
                        ignorePropertyValue);
            }
        }

        return propertiesToIgnore;
    }
    
    private static void checkTablesName(final Set<String> expectedTableNames, final List<String> dynamodbTableNames) {
        final Set<String> allTables = new HashSet<>(dynamodbTableNames);
        allTables.addAll(expectedTableNames);

        if (allTables.size() != expectedTableNames.size() || allTables.size() != dynamodbTableNames.size()) {
            throw FailureHandler.createFailure("Expected table names are %s but insert table names are %s",
                    expectedTableNames, dynamodbTableNames);
        }

    }
}
