package org.javlo.helper.sql.test.factory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class PGConnnectionFactory {

	private static PGConnnectionFactory instance;

	private static Logger logger = Logger.getLogger(PGConnnectionFactory.class.getName());

	public static PGConnnectionFactory getInstance() {
		if (instance == null) {
			instance = new PGConnnectionFactory();
		}
		return instance;
	}

	public Connection getConnection() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		Class.forName("org.postgresql.Driver");
		String url = "jdbc:postgresql://dev2.host.javlo.org/test?characterEncoding=utf8";
		Connection c = DriverManager.getConnection(url, "test",  System.getenv("DEFAULT_TEST_PASSWORD") );
		return c;
	}

	public void releaseConnection(Connection conn) throws SQLException {
		conn.close();
	}

	public void releaseConnection(Statement st, Connection conn) throws SQLException {
		if (st != null) {
			try {
				st.close();
			} catch (Throwable t) {
			}
		}
		conn.close();
	}

}
