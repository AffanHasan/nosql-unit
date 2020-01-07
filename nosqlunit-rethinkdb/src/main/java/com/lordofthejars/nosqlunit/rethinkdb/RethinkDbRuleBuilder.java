package com.lordofthejars.nosqlunit.rethinkdb;

/**
 * Builder class for {@link RethinkDbRuleBuilder} 
 * 
 * @author Affan Hasan
 */
public class RethinkDbRuleBuilder {
	
	private RethinkDbRule rethinkDbRule;
	
	/**
	 * Constructor to instantiate class instances.
	 */
	private RethinkDbRuleBuilder() {
		rethinkDbRule = new RethinkDbRule(RethinkDbConfigurationBuilder.rethinkDbConfig() //
				.build());
	}
	
	/**
	 * @return {@link RethinkDbRuleBuilder}
	 */
	public static RethinkDbRuleBuilder defaultRethinkDbRule() {
		return new RethinkDbRuleBuilder();
	}
	
	/**
	 * @return {@link RethinkDbRuleBuilder}
	 */
	public static RethinkDbRuleBuilder newRethinkDbRule() {
		return new RethinkDbRuleBuilder();
	}
	
	/**
	 * @param configuration {@link RethinkDbConfiguration}
	 * @return {@link RethinkDbRuleBuilder}
	 */
	public RethinkDbRuleBuilder config(final RethinkDbConfiguration configuration) {
		rethinkDbRule = new RethinkDbRule(configuration);
		return this;
	}
	
	/**
	 * Constructs new {@link RethinkDbRule}
	 * 
	 * @return {@link RethinkDbRule}
	 */
	public RethinkDbRule build() {
		return rethinkDbRule;
	}
}
