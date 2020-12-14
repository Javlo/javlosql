package org.javlo.helper.sql.test.factory;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

public class H2ConnectionFactory {

	private static final File BASE_FOLDER = new File(System.getProperty("user.home") + "/__test_javlo_sql");

	private static H2ConnectionFactory instance;

	private static Logger logger = Logger.getLogger(H2ConnectionFactory.class.getName());

	private String dbLogin = "sa";
	private String dbPassword = "";

	public static H2ConnectionFactory getInstance() {
		if (instance == null) {
			instance = new H2ConnectionFactory();
			try {
				if (BASE_FOLDER.exists()) {
					try {
						FileUtils.deleteDirectory(BASE_FOLDER);
					} catch (Throwable e) {
						// e.printStackTrace();
					}
				} else {
					BASE_FOLDER.mkdirs();
				}
			} catch (Throwable t) {
			}
		}
		return instance;
	}

	public Connection getConnection(String dbname) throws Exception {
		Connection conn;
		String url = "jdbc:h2:" + BASE_FOLDER.getAbsolutePath()+'/'+dbname;
		System.out.println(">>> create connection : "+url);
		try {
			conn = DriverManager.getConnection(url, dbLogin, dbPassword);
		} catch (Exception e) {
			loadDriver();
			conn = DriverManager.getConnection(url, dbLogin, dbPassword);
		}
		return conn;
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

	private void loadDriver() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		ClassLoader cl = H2ConnectionFactory.class.getClassLoader();
		if (cl != null) {
			logger.info("Loading custom class loader for H2 driver: " + cl.toString());
			Driver driver = (Driver) Class.forName("org.h2.Driver", true, cl).newInstance();
			logger.info("Loaded H2 driver: " + driver.toString() + " - " + driver.getMinorVersion() + " - " + driver.getMajorVersion());
			DriverManager.registerDriver(driver);
		} else {
			logger.info("Loading H2 driver.");
			Class.forName("org.h2.Driver");
		}
	}

}
