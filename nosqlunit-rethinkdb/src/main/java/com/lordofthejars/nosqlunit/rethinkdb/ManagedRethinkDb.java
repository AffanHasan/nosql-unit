package com.lordofthejars.nosqlunit.rethinkdb;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intended to be used as a class rule to start/stop remote RethinkDb process.
 *  
 * @author Affan Hasan
 */
public class ManagedRethinkDb extends ExternalResource {
	
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private Process process;
	
	public static final String START_RETHINKDB_COMMAND = "rethinkdb";
	
	@Override
	protected void before() throws Throwable {
		process = new ProcessBuilder(START_RETHINKDB_COMMAND).start();
		process.waitFor(3, SECONDS);
		logger.info("Successfully started RethinkDb process for integration tests");
	}
	
	@Override
	protected void after() {
		process.destroyForcibly();
		try {
			process.waitFor();
			if(process.isAlive())
				throw new IllegalStateException("Unable to shutdown RethinkDb process.");
		} catch (final Exception e) {
			logger.error(e.getMessage());
		}
		logger.info("Successfully stopped RethinkDb process for integration tests");
	}
}
