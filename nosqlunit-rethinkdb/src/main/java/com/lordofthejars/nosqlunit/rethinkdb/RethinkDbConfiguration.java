package com.lordofthejars.nosqlunit.rethinkdb;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

/**
 * Configurations required to connect a locally hosted RethinkDb instance. 
 * 
 * @author Affan Hasan
 */
public final class RethinkDbConfiguration extends AbstractJsr330Configuration {

    private static final String DEFAULT_HOST = "localhost";
    
    private static final Integer DEFAULT_PORT = 28015;
    
    private static final String DEFAULT_DATABASE = "test";
    
    private String host = DEFAULT_HOST;
    
    private Integer port = DEFAULT_PORT;
    
    private String database = DEFAULT_DATABASE;

    /**
     * Default constructor
     */
    public RethinkDbConfiguration() {
        super();
    }
    
    /**
     * Returns RethinkDb host address
     * 
     * @return {@link String} host
     */
    public String getHost() {
		return host;
	}

	/**
	 * Returns RethinkDb port address
	 * 
	 * @return {@link Integer}
	 */
	public Integer getPort() {
		return port;
	}
	
	/**
	 * Returns RethinkDb database name
	 * 
	 * @return {@link String} database name
	 */
	public String getDatabase() {
		return database;
	}

	/**
	 * Set RethinknDb host address
	 * 
	 * @param host {@link Integer}
	 */
	public void setHost(final String host) {
        this.host = host;
    }

    /**
     * Set RethinkDb port
     * 
     * @param port {@link Integer}
     */
    public void setPort(final Integer port) {
    	this.port = port;
    }
    
    /**
     * Set RethinkDb database name
     * 
     * @param database {@link String} database name
     */
    public void setDatabase(final String database) {
    	this.database = database;
    }
}
