package com.lordofthejars.nosqlunit.rethinkdb;

import static com.rethinkdb.RethinkDB.r;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.rethinkdb.net.Connection;

/**
 * This is core class which provides JUnit {@link Rule} to set RethinkDb to a specific state before the execution of an integration test.
 * 
 * @author Affan Hasan
 */
public class RethinkDbRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "json";
	
	protected DatabaseOperation<Connection> databaseOperation;
	
	private RethinkDbConfiguration rethinkDbConfiguration;
	
	/**
	 * Default constructor to create class instances.
	 */
	public RethinkDbRule() {
		super("sample Identifier");
	}
	
	/**
	 * Constructor to instantiate class instances.
	 * 
	 * @param configuration {@link RethinkDbConfiguration}
	 */
	public RethinkDbRule(final RethinkDbConfiguration configuration) {
		super("identifier");
		this.rethinkDbConfiguration = configuration;
		final Connection connection = r.connection() //
		.hostname(configuration.getHost()) //
		.port(configuration.getPort()) //
		.connect();
		this.databaseOperation = new RethinkDbOperation(() -> connection, rethinkDbConfiguration);
	}

	/* (non-Javadoc)
	 * @see com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule#getDatabaseOperation()
	 */
	@Override
	public DatabaseOperation<Connection> getDatabaseOperation() {
		return databaseOperation;
	}

	/* (non-Javadoc)
	 * @see com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule#getWorkingExtension()
	 */
	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}

	/* (non-Javadoc)
	 * @see com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule#close()
	 */
	@Override
	public void close() {
		databaseOperation.connectionManager() //
		.close();
	}
}
