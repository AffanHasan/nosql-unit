package com.lordofthejars.nosqlunit.rethinkdb;

import static com.rethinkdb.RethinkDB.*;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lordofthejars.nosqlunit.core.InsertionStrategy;

/**
 * Default implementation of {@link InsertionStrategy}
 * 
 * @author Affan Hasan
 */
public class DefaultInsertionStrategy implements InsertionStrategy<RethinkDbConnectionCallback> {

	protected final Logger logger = LoggerFactory.getLogger(getClass());
	
	/* (non-Javadoc)
	 * @see com.lordofthejars.nosqlunit.core.InsertionStrategy#insert(java.lang.Object, java.io.InputStream)
	 */
	@Override
	public void insert(final RethinkDbConnectionCallback connectionCallback, final InputStream dataset) throws Throwable {
		final ObjectMapper objectMapper = new ObjectMapper();
        final Map<String, List<Map<String, Object>>> parsedData = objectMapper.readValue(dataset, 
        		new TypeReference<Map<String, List<Map<String, Object>>>>() {
		});
        // Extract table names from data file
        final List<String> tableNames = parsedData.keySet() //
        		.stream() //
        		.collect(Collectors.toList());
        // Create tables if not exists
		final List<String> databaseTables = r.tableList() //
		.run(connectionCallback.dbConnection());
		tableNames.stream() //
		.forEach(table->{
			if(!databaseTables.contains(table)) {
				r.tableCreate(table) //
				.run(connectionCallback.dbConnection());
			}
		});
		// Insert documents into respective table
		tableNames.forEach(table->
			r.table(table) //
			.insert(parsedData.get(table)) //
			.run(connectionCallback.dbConnection())
		);
	}
}
