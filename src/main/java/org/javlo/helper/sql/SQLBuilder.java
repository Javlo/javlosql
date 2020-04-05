package org.javlo.helper.sql;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class SQLBuilder {

	private static Logger logger = Logger.getLogger(SQLBuilder.class.getName());

	private static final String getResultSetGetMethod(String type) {
		if (type.equalsIgnoreCase("String")) {
			return "getString";
		} else if (type.equalsIgnoreCase("Integer") || type.equalsIgnoreCase("int")) {
			return "getInt";
		} else if (type.equalsIgnoreCase("Long")) {
			return "getLong";
		} else if (type.equalsIgnoreCase("Date")) {
			return "getDate";
		} else if (type.equalsIgnoreCase("LocalDate")) {
			return "getDate";
		} else if (type.equalsIgnoreCase("Timestamp")) {
			return "getTimestamp";
		} else if (type.equalsIgnoreCase("Double") || type.equalsIgnoreCase("Float")) {
			return "getDouble";
		} else if (type.equalsIgnoreCase("boolean")) {
			return "getBoolean";
		} else if (type.equalsIgnoreCase("time")) {
			return "getTime";
		} else {
			logger.warning("type not found : " + type);
			return "getString";
		}
	}

	private static final Method getStatementSetMethod(PreparedStatement st, String type) throws NoSuchMethodException, SecurityException {
		if (type.equalsIgnoreCase("String")) {
			return st.getClass().getMethod("setString", int.class, String.class);
		} else if (type.equalsIgnoreCase("Integer") || type.equalsIgnoreCase("int")) {
			return st.getClass().getMethod("setInt", int.class, int.class);
		} else if (type.equalsIgnoreCase("Long")) {
			return st.getClass().getMethod("setLong", int.class, long.class);
		} else if (type.equalsIgnoreCase("Date")) {
			return st.getClass().getMethod("setDate", int.class, java.sql.Date.class);
		} else if (type.equalsIgnoreCase("Time")) {
			return st.getClass().getMethod("setTime", int.class, java.sql.Time.class);
		} else if (type.equalsIgnoreCase("timestamp")) {
			return st.getClass().getMethod("setTimestamp", int.class, java.sql.Timestamp.class);
		} else if (type.equalsIgnoreCase("double")) {
			return st.getClass().getMethod("setDouble", int.class, double.class);
		} else if (type.equalsIgnoreCase("boolean")) {
			return st.getClass().getMethod("setBoolean", int.class, boolean.class);
		} else if (type.equalsIgnoreCase("LocalDate")) {
			return st.getClass().getMethod("setDate", int.class, java.sql.Date.class);
		} else {
			logger.warning("type not found : " + type);
			return st.getClass().getMethod("setString", int.class, String.class);
		}
	}
	
	public static boolean isEmpty(Object value) {
		return (value == null || value.toString() == null || value.toString().trim().length() == 0);
	}

	
	public static String neverNullOrEmpty(Object inStr, String replaceWith) {
		if (isEmpty(inStr)) {
			return replaceWith;
		} else {
			return "" + inStr;
		}
	}
	
	public static <E> List<E> createList(E... items) {
		List<E> out = new LinkedList<>();
		for (E i : items) {
			out.add(i);
		}
		return out;
	}

	public static boolean isIdExist(Connection conn, String table, Long id) throws SQLException {
		Statement st = conn.createStatement();
		try {
			ResultSet rs = st.executeQuery("SELECT id FROM \"" + table + "\" WHERE id=" + id);
			return rs.next();
		} finally {
			st.close();
		}
	}

	public static boolean isLineExist(Connection conn, String table, Collection<SQLItem> whereItem) throws SQLException {
		Statement st = conn.createStatement();
		try {
			ResultSet rs = st.executeQuery("SELECT " + whereItem.iterator().next().getName() + " FROM \"" + table + "\" " + createWereClause(whereItem));
			return rs.next();
		} finally {
			st.close();
		}
	}

	public static List<SQLItem> extractSQLItemFromBean(Object bean, boolean widthAuto) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		List<SQLItem> items = new LinkedList<>();
		for (Method m : bean.getClass().getMethods()) {
			if (m.getName().startsWith("get") || m.getName().startsWith("is")) {
				Column a = m.getAnnotation(Column.class);
				if (a != null) {
					if (widthAuto || !a.auto()) {
						String type = neverNullOrEmpty(a.type(), m.getReturnType().getSimpleName());
						String name = neverNullOrEmpty(a.name(), getAttributeName(m.getName()));
						Object value = m.invoke(bean);
						items.add(new SQLItem(name, type, value, a.primaryKey(), a.foreign(), a.notNull(), a.auto(), a.defaultValue()));
					}
				}
			}
		}
		return items;
	}

	private static String getAttributeName(String name) {
		if (name.startsWith("get")) {
			name = name.substring(3);
		} else if (name.startsWith("is")) {
			name = name.substring(2);
		}
		name = name.substring(0, 1).toLowerCase() + name.substring(1);
		return name;
	}

	private static Object convertToJava(Object o) {
		if (o instanceof Timestamp) {
			return new Date(((Timestamp) o).getTime());
		} else if (o instanceof java.sql.Date) {
			return new Date(((java.sql.Date) o).getTime());
		} else {
			return o;
		}
	}

	public static Object rsToBean(ResultSet rs, Object bean) throws SQLException {
		String type = null;
		String name = null;
		try {
			for (Method m : bean.getClass().getMethods()) {
				if (m.getName().startsWith("get") || m.getName().startsWith("is")) {
					Column a = m.getAnnotation(Column.class);
					if (a != null) {
						type = neverNullOrEmpty(a.type(), m.getReturnType().getSimpleName());
						name = neverNullOrEmpty(a.name(), getAttributeName(m.getName()));
						Method rsMethod = rs.getClass().getMethod(getResultSetGetMethod(type), String.class);
						Object value = convertToJava(rsMethod.invoke(rs, name));
						boolean wasNull = rs.wasNull();
						Method setMethod;
						if (m.getName().startsWith("get")) {
							setMethod = bean.getClass().getMethod(m.getName().replaceFirst("get", "set"), m.getReturnType());
						} else {
							setMethod = bean.getClass().getMethod(m.getName().replaceFirst("is", "set"), m.getReturnType());
						}

						if (wasNull && setMethod.getParameters().length > 0 && !setMethod.getParameterTypes()[0].isPrimitive()) {
							setMethod.invoke(bean, new Object[] { null });
						} else {
							setMethod.invoke(bean, value);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage() + " [" + name + " - " + type + "]");
		}
		return bean;
	}

	public static Long insert(Connection conn, Object bean, SQLItem... manualItems) throws SQLException {
		Table t = bean.getClass().getAnnotation(Table.class);
		String tableName = bean.getClass().getSimpleName().toLowerCase();
		if (t != null && !isEmpty(t.name())) {
			tableName = t.name();
		}
		try {
			List<SQLItem> items = extractSQLItemFromBean(bean, false);
			items.addAll(createList(manualItems));
			return insert(conn, tableName, items);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public static Long insert(Connection conn, String table, Collection<SQLItem> items) throws SQLException {
		String sql = "INSERT INTO \"" + table + "\" (";
		String valuesSQL = "";
		Long outId = null;
		for (SQLItem i : items) {
			sql = sql + '"' + i.getName() + "\",";
			valuesSQL = valuesSQL + "?,";
		}
		sql = sql.substring(0, sql.length() - 1) + ") VALUES (" + valuesSQL.substring(0, valuesSQL.length() - 1) + ")";
		if (conn != null) {
			PreparedStatement st = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			try {
				int i = 1;
				for (SQLItem item : items) {
					Method set = getStatementSetMethod(st, item.getType());
					if (item.getValue() != null) {
						try {
							set.invoke(st, i, item.getValue());
						} catch (Exception e) {
							throw new SQLException("error on : "+item.getName()+"::"+item.getType()+" method:"+set.getName()+" method_type:"+set.getParameters()[1].getType()+" value_type="+item.getValue().getClass().getName()+" - "+e.getMessage());
						}
						
					} else {
						st.setNull(i, item.getSQLType());
					}
					i++;
				}
				st.execute();
				ResultSet generatedKeys = st.getGeneratedKeys();
				if (generatedKeys.next()) {
					Object newId = generatedKeys.getObject(1);
					if (newId instanceof Long) {
						outId = (long) newId;
					} else if (newId instanceof Integer) {
						int newIdInt = (int) newId;
						outId = (long) newIdInt;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new SQLException(e);
			} finally {
				st.close();
			}
		} else { // DEBUG
			System.out.println("SQL = " + sql);
		}
		return outId;
	}

	private static String getTableName(Object bean) {
		Table t = bean.getClass().getAnnotation(Table.class);
		String tableName = bean.getClass().getSimpleName().toLowerCase();
		if (t != null && !isEmpty(t.name())) {
			tableName = t.name();
		}
		return tableName;
	}

	public static Long update(Connection conn, Collection<SQLItem> whereIds, Object bean, Collection<SQLItem> inItems) throws SQLException {
		String tableName = getTableName(bean);
		try {
			List<SQLItem> items = extractSQLItemFromBean(bean, false);
			if (inItems != null) {
				items.addAll(inItems);
			}
			return update(conn, whereIds, tableName, items);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	private static final String createWereClause(Collection<SQLItem> whereIds) {
		String where = " WHERE ";
		for (SQLItem i : whereIds) {
			if (i.getValue() == null) {
				return null;
			}
			if (i.getType().equalsIgnoreCase("string")) {
				where = where + '"' + i.getName() + "\"='" + i.getValue() + "' AND ";
			} else {
				where = where + '"' + i.getName() + "\"=" + i.getValue() + " AND ";
			}
		}
		where = where.substring(0, where.length() - " AND ".length());
		return where;
	}

	public static Object load(Connection conn, Object bean) throws SQLException {
		List<SQLItem> items;
		try {
			items = extractSQLItemFromBean(bean, true);
		} catch (Exception e) {
			throw new SQLException(e);
		}
		List<SQLItem> whereItems = new LinkedList<>();
		for (SQLItem sqlItem : items) {
			if (sqlItem.isPrimaryKey()) {
				whereItems.add(sqlItem);
			}
		}
		String tableName = getTableName(bean);
		Statement st = conn.createStatement();
		try {
			ResultSet rs = st.executeQuery("select * from \"" + tableName + "\" " + createWereClause(whereItems));
			if (rs.next()) {
				rsToBean(rs, bean);
			} else {
				return null;
			}
			if (rs.next()) {
				throw new SQLException("more than one result on load bean : " + bean);
			}
			return bean;
		} finally {
			st.close();
		}
	}
	
	public static boolean update(Connection conn, Object bean, String... inWhereCols) throws SQLException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Collection<String> whereCols = Arrays.asList(inWhereCols);
		List<SQLItem> items = extractSQLItemFromBean(bean, true);
		List<SQLItem> whereItems = new LinkedList<>();
		for (SQLItem sqlItem : items) {
			if (sqlItem.isPrimaryKey() || whereCols.contains(sqlItem.getName())) {
				whereItems.add(sqlItem);
			}
		}
		if (whereItems.size() == 0) {
			throw new SQLException("no where clause defined in bean : "+bean.getClass().getCanonicalName());
		}
		Statement st = conn.createStatement();
		try {
			update(conn, whereItems, bean, null);
			return true;
		} finally {
			st.close();
		}
	}

	/**
	 * create or insert the bean in the table
	 * 
	 * @param conn
	 *            a connection to the data base
	 * @param bean
	 *            a bean with @table
	 * @return true if insert, false if update
	 * @throws SQLException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 */
	public static boolean insertOrUpdate(Connection conn, Object bean) throws SQLException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		List<SQLItem> items = extractSQLItemFromBean(bean, true);
		List<SQLItem> whereItems = new LinkedList<>();
		for (SQLItem sqlItem : items) {
			if (sqlItem.isPrimaryKey()) {
				whereItems.add(sqlItem);
			}
		}
		if (whereItems.size() == 0) {
			throw new SQLException("no where clause defined in bean");
		}
		String tableName = getTableName(bean);
		Statement st = conn.createStatement();
		try {
			boolean update = false;
			String where = createWereClause(whereItems);
			if (where != null) {
				ResultSet rs = st.executeQuery("select * from \"" + tableName + "\" " + where);
				if (rs.next()) {
					update = true;
				}
			}
			if (update) {
				update(conn, whereItems, bean, null);
				return false;
			} else {
				insert(conn, bean);
				return true;
			}
		} finally {
			st.close();
		}
	}

	public static Long insertOrUpdate(Connection conn, Collection<SQLItem> whereIds, String table, Collection<SQLItem> items, Collection<SQLItem> insertItem, Collection<SQLItem> updateItem) throws SQLException {
		return insertOrUpdate(conn, whereIds, table, items, insertItem, updateItem, true);
	}

	public static Long insertOrUpdate(Connection conn, Collection<SQLItem> whereIds, String table, Collection<SQLItem> items, Collection<SQLItem> insertItem, Collection<SQLItem> updateItem, boolean generateKey) throws SQLException {
		Statement st = conn.createStatement();
		boolean update;
		try {
			String sql = "SELECT \"" + whereIds.iterator().next().getName() + "\" FROM \"" + table + "\"" + createWereClause(whereIds);
			ResultSet rs = st.executeQuery(sql);
			update = rs.next();
		} finally {
			st.close();
		}
		if (update) {
			if (updateItem != null) {
				items.addAll(updateItem);
			}
			update(conn, whereIds, table, items, generateKey);
			return (long) 0;
		} else {
			if (insertItem != null) {
				items.addAll(insertItem);
			}
			return insert(conn, table, items);
		}
	}

	public static Long update(Connection conn, Collection<SQLItem> whereIds, String table, Collection<SQLItem> items) throws SQLException {
		return update(conn, whereIds, table, items, true);
	}

	public static Long update(Connection conn, Collection<SQLItem> whereIds, String table, Collection<SQLItem> items, boolean generateKey) throws SQLException {
		assert (whereIds.size() > 0);
		String sql = "UPDATE \"" + table + "\" SET ";
		Long outId = null;
		for (SQLItem i : items) {
			sql = sql + '"' + i.getName() + "\"=?,";
		}

		sql = sql.substring(0, sql.length() - 1) + createWereClause(whereIds);
		if (conn != null) {
			PreparedStatement st = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			try {
				int i = 1;
				for (SQLItem item : items) {
					Method set = getStatementSetMethod(st, item.getType());
					if (item.getValue() != null) {
						set.invoke(st, i, item.getValue());
					} else {
						st.setNull(i, item.getSQLType());
					}
					i++;
				}
				st.execute();
				if (generateKey) {
					ResultSet generatedKeys = st.getGeneratedKeys();
					if (generatedKeys.next()) {
						try {
							outId = generatedKeys.getLong(1);
						} catch (Exception e) {

						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				logger.severe("error in : " + sql);
				throw new SQLException(e);
			} finally {
				st.close();
			}
		} else { // DEBUG
			System.out.println("SQL = " + sql);
		}
		return outId;
	}

	public static Map<Integer, String> getAllJdbcTypeNames() throws IllegalArgumentException, IllegalAccessException {
		Map<Integer, String> result = new HashMap<Integer, String>();
		for (Field field : Types.class.getFields()) {
			result.put((Integer) field.get(null), field.getName());
		}
		return result;
	}
	
	public static Object escapeForSql(Object value) {
		if (value == null || !(value instanceof String)) {
			return value;
		} else {
			return ((String)value).replace("'", "''");
		}
	}

	public static void createOrUpdateTable(Connection conn, Object bean) throws SQLException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		String tableName = getTableName(bean);
		Statement st = conn.createStatement();
		Map<Integer, String> sqlTypes = getAllJdbcTypeNames();
		try {
			st.execute("CREATE TABLE \"" + tableName + "\" ()");
		} catch (SQLException e) {
			// e.printStackTrace();
		}
		for (SQLItem item : extractSQLItemFromBean(bean, true)) {
			String sql = "ALTER TABLE \"" + tableName + "\" ADD COLUMN \"" + item.getName() + "\" ";
			if (item.isAuto()) {
				sql += "SERIAL";
			} else {
				sql += sqlTypes.get(item.getSQLType());
			}
			try {
				st.execute(sql);
			} catch (Exception e) {
				// e.printStackTrace();
			}
			if (item.isPrimaryKey()) {
				sql = "ALTER TABLE \"" + tableName + "\" ADD PRIMARY KEY (\"" + item.getName() + "\")";
			}
			try {
				st.execute(sql);
			} catch (Exception e) {
				// e.printStackTrace();
			}
			
			if (item.isNotNull()) {
				sql = "ALTER TABLE \"" + tableName + "\" ALTER COLUMN \"" + item.getName() + "\" SET NOT NULL";
			} else {
				sql = "ALTER TABLE \"" + tableName + "\" ALTER COLUMN \"" + item.getName() + "\" DROP NOT NULL";
			}
			try {
				st.execute(sql);
			} catch (Exception e) {
				// e.printStackTrace();
			}
			
			if (!isEmpty(item.getDefaultValue())) {
				if (isNumeric(item.getType())) {
					sql = "ALTER TABLE \"" + tableName + "\" ALTER \"" + item.getName() + "\" SET DEFAULT "+item.getDefaultValue();
				} else {
					sql = "ALTER TABLE \"" + tableName + "\" ALTER \"" + item.getName() + "\" SET DEFAULT '"+item.getDefaultValue()+"'";
				}
			}
			try {
				st.execute(sql);
			} catch (Exception e) {
				// e.printStackTrace();
			}
			
			if (!isEmpty(item.getForeign())) {
				if (!item.getForeign().contains(".")) {
					throw new SQLException("foreign format : #table#.#column#");
				}
				sql = "ALTER TABLE \"" + tableName + "\" ADD CONSTRAINT " + item.getName() + "_foreign FOREIGN KEY (\"" + item.getName() + "\") REFERENCES \"" + item.getForeign().split("\\.")[0] + "\" (" + item.getForeign().split("\\.")[1] + ") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE";
				System.out.println(sql);
				try {
					st.execute(sql);
				} catch (Exception e) {
					// e.printStackTrace();
				}
			}
		}
		st.close();
	}

	private static boolean isNumeric(String type) {
		if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("long") || type.equalsIgnoreCase("real")) {
			return true;
		} else {
			return false;
		}
	}
}
