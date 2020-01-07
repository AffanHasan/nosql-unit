package com.lordofthejars.nosqlunit.rethinkdb;

/**
 * Builder for {@link ManagedRethinkDbServerRuleBuilder}
 * 
 * @author Affan Hasan
 */
public class ManagedRethinkDbServerRuleBuilder {

	private ManagedRethinkDb managedRethinkDbRule;
	
	private ManagedRethinkDbServerRuleBuilder() {
		managedRethinkDbRule = new ManagedRethinkDb();
	}
	
	/**
	 * Returns an instance of {@link ManagedRethinkDbServerRuleBuilder}
	 * 
	 * @return {@link ManagedRethinkDbServerRuleBuilder}
	 */
	public static ManagedRethinkDbServerRuleBuilder newManagedRethinkDbRule() {
		return new ManagedRethinkDbServerRuleBuilder();
	}
	
	/**
	 * Creates an instance of {@link ManagedRethinkDb} 
	 * 
	 * @return {@link ManagedRethinkDb}
	 */
	public ManagedRethinkDb build() {
		return managedRethinkDbRule;
	}
}
