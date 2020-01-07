package com.lordofthejars.nosqlunit.rethinkdb;

import com.rethinkdb.net.Connection;

/**
 * Contains method to retrieve {@link Connection}
 * 
 * @author Affan Hasan
 */
public interface RethinkDbConnectionCallback {

    Connection dbConnection();
}
