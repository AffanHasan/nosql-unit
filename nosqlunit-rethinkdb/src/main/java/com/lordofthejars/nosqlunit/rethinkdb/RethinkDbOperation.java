package com.lordofthejars.nosqlunit.rethinkdb;

import static com.rethinkdb.RethinkDB.r;

import java.io.InputStream;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.rethinkdb.net.Connection;

/**
 * An important class which performs insertion/deletions of test data to the underlying database. 
 * 
 * @author Affan Hasan
 */
public class RethinkDbOperation extends AbstractCustomizableDatabaseOperation<RethinkDbConnectionCallback, Connection> {

	private final RethinkDbConnectionCallback rethinkDbConnectionCallback;
	
	private RethinkDbConfiguration rethinkDbConfiguration;
	
	/**
	 * Constructor to instantiate class instances.
	 * 
	 * @param rethinkDbConnectionCallback {@link RethinkDbConnectionCallback}
	 * @param rethinkDbConfiguration {@link RethinkDbConfiguration}
	 */
	public RethinkDbOperation(final RethinkDbConnectionCallback rethinkDbConnectionCallback, final RethinkDbConfiguration rethinkDbConfiguration) {
		this.rethinkDbConnectionCallback = rethinkDbConnectionCallback;
		this.setInsertionStrategy(new DefaultInsertionStrategy());
		this.setComparisonStrategy(new DefaultRethinkComparisionStrategy());
		this.rethinkDbConfiguration = rethinkDbConfiguration;
	}
	
	/* (non-Javadoc)
	 * @see com.lordofthejars.nosqlunit.core.DatabaseOperation#insert(java.io.InputStream)
	 */
	@Override
	public void insert(final InputStream contentStream) {
        try {
            executeInsertion((rethinkDbConnectionCallback::dbConnection), contentStream);
        } catch (Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading data set file.", e);
        }
	}

	/* (non-Javadoc)
	 * @see com.lordofthejars.nosqlunit.core.DatabaseOperation#deleteAll()
	 */
	@Override
	public void deleteAll() {
		r.dbDrop(rethinkDbConfiguration.getDatabase()) //
		.run(rethinkDbConnectionCallback.dbConnection());
		r.dbCreate(rethinkDbConfiguration.getDatabase()) //
		.run(rethinkDbConnectionCallback.dbConnection());
	}

	/* (non-Javadoc)
	 * @see com.lordofthejars.nosqlunit.core.DatabaseOperation#databaseIs(java.io.InputStream)
	 */
	@Override
	public boolean databaseIs(final InputStream expectedData) {
        try {
            executeComparison(rethinkDbConnectionCallback::dbConnection, expectedData);
            return true;
        } catch (NoSqlAssertionError e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading expected data set file.", e);
        }
	}

	/* (non-Javadoc)
	 * @see com.lordofthejars.nosqlunit.core.DatabaseOperation#connectionManager()
	 */
	@Override
	public Connection connectionManager() {
		return rethinkDbConnectionCallback.dbConnection();
	}

}
