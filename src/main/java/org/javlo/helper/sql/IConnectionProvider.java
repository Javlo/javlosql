package org.javlo.helper.sql;

import java.sql.Connection;
import java.sql.SQLException;

public interface IConnectionProvider {
	
	public Connection getConnection() throws SQLException;
	
	public void releaseConnection(Connection conn);

}
