package com.lordofthejars.nosqlunit.rethinkdb;

/**
 * Configuration builder for {@link RethinkDbConfiguration}
 * 
 * @author Affan Hasan
 */
public class RethinkDbConfigurationBuilder {
	
	private final RethinkDbConfiguration rethinkDbConfiguration;
	
	public static RethinkDbConfigurationBuilder rethinkDbConfig(){
		return new RethinkDbConfigurationBuilder();
	}
	
	private RethinkDbConfigurationBuilder(){
		rethinkDbConfiguration = new RethinkDbConfiguration();
	}
	
	public RethinkDbConfigurationBuilder host(final String hostName) {
		rethinkDbConfiguration.setHost(hostName);
		return this;
	}
	
	public RethinkDbConfigurationBuilder port(final Integer port) {
		rethinkDbConfiguration.setPort(port);
		return this;
	}
	
	public RethinkDbConfigurationBuilder database(final String database) {
		rethinkDbConfiguration.setDatabase(database);
		return this;
	}
	
	public RethinkDbConfiguration build() {
		return rethinkDbConfiguration;
	}
}
