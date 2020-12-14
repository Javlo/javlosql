package org.javlo.helper.sql.test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.javlo.helper.sql.SQLBuilder;
import org.javlo.helper.sql.test.factory.PGConnnectionFactory;

import junit.framework.TestCase;

public class SqlTest extends TestCase {

	private static final String DB_NAME = "_test_";
	private static final PGConnnectionFactory connectionFact = PGConnnectionFactory.getInstance();

	public void testCreate() throws Exception {

		/** INIT DB **/

		Connection conn = connectionFact.getConnection();
		Statement st = conn.createStatement();
		try {
		st.execute("drop table \"user\"");
		st.execute("drop table \"city\"");
		} catch (Throwable  t) {}
		
		SQLBuilder.createOrUpdateTable(conn, new User(), false);
		SQLBuilder.createOrUpdateTable(conn, new City(), false);
		
		PreparedStatement testSt = conn.prepareStatement("insert into city (name) values(?)");
		System.out.println(">>>>>>>>> SqlTest.testCreate : testSt class = "+testSt.getClass()); //TODO: remove debug trace
		Method setString = testSt.getClass().getMethod("setString", int.class, String.class);
		setString.invoke(testSt, 1, "TEST_CITY");
		//testSt.setString(1, "TEST_CITY");
		testSt.execute();
		testSt.close();
		

		City c = new City();
		String name = "Brussels";		
		c.setName(name);
		SQLBuilder.insert(conn, c);
		ResultSet rs = st.executeQuery("select * from \"city\"");
		if (rs.next()) {
			c = (City) SQLBuilder.rsToBean(rs, c);
			assert(c.getId()>0);
			assertEquals(c.getName(), name);
		} else {
			assert(false);
		}		
		
		User u = new User();
		String firstname = "Jean-Claude";
		String lastname = "Van Damme";
		String email = "test@javlo.org";
		u.setFirstname(firstname);
		u.setLastname(lastname);
		u.setEmail(email);
		SQLBuilder.insert(conn, u);		
		st = conn.createStatement();
		rs = st.executeQuery("select * from \"user\"");
		if (rs.next()) {
			u = (User) SQLBuilder.rsToBean(rs, u);
			assert(u.getId()>0);
			assertEquals(u.getFirstname(), firstname);
			assertEquals(u.getLastname(), lastname);
			assertEquals(u.getEmail(), email);
		} else {
			assert(false);
		}
		
		/** foreign **/
		st.execute("delete from \"city\"");
		rs = st.executeQuery("select * from \"city\"");
		assertFalse(rs.next());
		rs = st.executeQuery("select * from \"user\"");
		assertFalse(rs.next());
		
		st.close();
		conn.close();

	}

}
