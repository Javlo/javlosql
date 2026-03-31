package org.javlo.helper.sql;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class SQLBuilder {

	public static boolean DEBUG = false;

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
		} else if (type.equalsIgnoreCase("LocalTime")) {
			return "getTime";
		} else if (type.equalsIgnoreCase("Timestamp") || type.equalsIgnoreCase("LocalDateTime")) {
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

	/**
	 * Binds a SQLItem value to a PreparedStatement parameter at the given index.
	 * Replaces reflection-based approach for better performance and type safety.
	 */
	private static void bindParam(PreparedStatement st, int index, SQLItem item) throws SQLException {
		if (item.getValue() == null) {
			st.setNull(index, item.getSQLType());
			return;
		}
		Object value = item.getValue();
		if (value instanceof LocalDate) {
			value = java.sql.Date.valueOf((LocalDate) value);
		} else if (value instanceof LocalDateTime) {
			value = Timestamp.valueOf((LocalDateTime) value);
		} else if (value instanceof LocalTime) {
			value = java.sql.Time.valueOf((LocalTime) value);
		}
		String type = item.getType();
		if (type.equalsIgnoreCase("String")) {
			st.setString(index, (String) value);
		} else if (type.equalsIgnoreCase("Integer") || type.equalsIgnoreCase("int")) {
			st.setInt(index, ((Number) value).intValue());
		} else if (type.equalsIgnoreCase("Long")) {
			st.setLong(index, ((Number) value).longValue());
		} else if (type.equalsIgnoreCase("Date") || type.equalsIgnoreCase("LocalDate")) {
			st.setDate(index, (java.sql.Date) value);
		} else if (type.equalsIgnoreCase("Time") || type.equalsIgnoreCase("LocalTime")) {
			st.setTime(index, (java.sql.Time) value);
		} else if (type.equalsIgnoreCase("Timestamp") || type.equalsIgnoreCase("LocalDateTime")) {
			st.setTimestamp(index, (Timestamp) value);
		} else if (type.equalsIgnoreCase("Double") || type.equalsIgnoreCase("Float")) {
			st.setDouble(index, ((Number) value).doubleValue());
		} else if (type.equalsIgnoreCase("boolean")) {
			st.setBoolean(index, (Boolean) value);
		} else {
			logger.severe("type not found for binding: " + type);
			st.setString(index, value.toString());
		}
	}

	/**
	 * Binds WHERE clause parameters to a PreparedStatement starting at startIndex.
	 * Returns the next available parameter index.
	 */
	private static int bindWhereParams(PreparedStatement st, Collection<SQLItem> whereIds, int startIndex) throws SQLException {
		int i = startIndex;
		for (SQLItem item : whereIds) {
			bindParam(st, i++, item);
		}
		return i;
	}

	public static boolean isEmpty(Object value) {
		return (value == null || value.toString().trim().length() == 0);
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
			if (i != null) {
				out.add(i);
			}
		}
		return out;
	}

	public static boolean isIdExist(IConnectionProvider connProv, String table, Long id) throws SQLException {
		Connection conn = connProv.getConnection();
		try {
			return isIdExist(conn, table, id);
		} finally {
			connProv.releaseConnection(conn);
		}
	}

	public static boolean isIdExist(Connection conn, String table, Long id) throws SQLException {
		PreparedStatement st = conn.prepareStatement("SELECT id FROM \"" + table + "\" WHERE id=?");
		try {
			st.setLong(1, id);
			try (ResultSet rs = st.executeQuery()) {
				return rs.next();
			}
		} finally {
			st.close();
		}
	}

	public static boolean isLineExist(Connection conn, String table, Collection<SQLItem> whereItem) throws SQLException {
		String sql = "SELECT \"" + whereItem.iterator().next().getName() + "\" FROM \"" + table + "\"" + createWereClause(whereItem);
		PreparedStatement st = conn.prepareStatement(sql);
		try {
			bindWhereParams(st, whereItem, 1);
			try (ResultSet rs = st.executeQuery()) {
				return rs.next();
			}
		} finally {
			st.close();
		}
	}

	public static List<SQLItem> extractSQLItemFromBean(Object bean, boolean widthAuto) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		List<SQLItem> items = new LinkedList<>();
		Table t = bean.getClass().getAnnotation(Table.class);
		for (Method m : bean.getClass().getMethods()) {
			if (m.getName().startsWith("get") || m.getName().startsWith("is")) {
				Column a = m.getAnnotation(Column.class);
				if (a != null) {
					if (widthAuto || !a.auto()) {
						String type = neverNullOrEmpty(a.type(), m.getReturnType().getSimpleName());
						String name;
						if (!StringHelperSql.isEmpty(a.name())) {
							name = a.name();
						} else {
							name = getAttributeName(m.getName());
							if (t != null && t.camelToSnake()) {
								name = StringHelperSql.camelToSnake(name);
							}
						}
						Object value = m.invoke(bean);
						items.add(new SQLItem(name, type, value, a.primaryKey(), a.foreign(), a.notNull(), a.unique(), a.auto(), a.order(), a.defaultValue()));
					}
				}
			}
		}
		Collections.sort(items, (o1, o2) -> {
			return o1.getOrder() - o2.getOrder();
		});
		return items;
	}

	public static SQLItem getPrimaryKey(Object bean) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Table t = bean.getClass().getAnnotation(Table.class);
		for (Method m : bean.getClass().getMethods()) {
			if (m.getName().startsWith("get")) {
				Column a = m.getAnnotation(Column.class);
				if (a != null) {
					if (a.primaryKey()) {
						String type = neverNullOrEmpty(a.type(), m.getReturnType().getSimpleName());
						String name;
						if (!StringHelperSql.isEmpty(a.name())) {
							name = a.name();
						} else {
							name = getAttributeName(m.getName());
							if (t != null && t.camelToSnake()) {
								name = StringHelperSql.camelToSnake(name);
							}
						}
						Object value = m.invoke(bean);
						return new SQLItem(name, type, value, a.primaryKey(), a.foreign(), a.notNull(), a.unique(), a.auto(), a.order(), a.defaultValue());
					}
				}
			}
		}
		return null;
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

	private static Object convertToJava(Object o, Class javaClass) {
		if (o == null) {
			return null;
		}
		if (javaClass.equals(LocalDate.class)) {
			java.sql.Date date = (java.sql.Date) o;
			return date.toLocalDate();
		}
		if (javaClass.equals(LocalDateTime.class)) {
			Timestamp ts = (Timestamp) o;
			return ts.toLocalDateTime();
		}
		if (javaClass.equals(LocalTime.class)) {
			Time ts = (Time) o;
			return ts.toLocalTime();
		}
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
			Table t = bean.getClass().getAnnotation(Table.class);
			for (Method m : bean.getClass().getMethods()) {
				if (m.getName().startsWith("get") || m.getName().startsWith("is")) {
					Column a = m.getAnnotation(Column.class);
					if (a != null) {
						type = neverNullOrEmpty(a.type(), m.getReturnType().getSimpleName());
						if (!StringHelperSql.isEmpty(a.name())) {
							name = a.name();
						} else {
							name = getAttributeName(m.getName());
							if (t != null && t.camelToSnake()) {
								name = StringHelperSql.camelToSnake(name);
							}
						}
						Method rsMethod = rs.getClass().getMethod(getResultSetGetMethod(type), String.class);
						Object value = convertToJava(rsMethod.invoke(rs, name), m.getReturnType());
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
			throw new SQLException(e.getMessage() + " [" + name + " - " + type + "]", e);
		}
		return bean;
	}

	public static Long insert(Connection conn, Object bean, SQLItem... manualItems) throws SQLException {
		Table t = bean.getClass().getAnnotation(Table.class);
		String tableName = getTableName(bean.getClass());
		if (t != null && !isEmpty(t.name())) {
			tableName = t.name();
		}
		try {
			List<SQLItem> items = extractSQLItemFromBean(bean, true);
			items.addAll(createList(manualItems));
			return insert(conn, tableName, items);
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public static Long insert(Connection conn, String table, Collection<SQLItem> items) throws SQLException {
		String sql = "INSERT INTO \"" + table + "\" (";
		String valuesSQL = "";
		Long outId = null;
		SQLItem primaryKey = null;
		for (SQLItem i : items) {
			if (i != null) {
				if (i.isAuto()) {
					primaryKey = i;
				} else {
					sql = sql + '"' + i.getName() + "\",";
					valuesSQL = valuesSQL + "?,";
				}
			}
		}
		sql = sql.substring(0, sql.length() - 1) + ") VALUES (" + valuesSQL.substring(0, valuesSQL.length() - 1) + ")";
		SQLItem errorItem = null;
		if (conn != null) {
			PreparedStatement st = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			try {
				int i = 1;
				for (SQLItem item : items) {
					errorItem = item;
					if (item != null && !item.isAuto()) {
						try {
							bindParam(st, i, item);
						} catch (Exception e) {
							throw new SQLException("error on : " + item.getName() + ":" + item.getType() + " value=" + item.getValue() + " - " + e.getMessage(), e);
						}
						i++;
					}
				}
				errorItem = null;
				st.execute();
				try (ResultSet generatedKeys = st.getGeneratedKeys()) {
					if (generatedKeys.next()) {
						Object newId;
						if (primaryKey != null) {
							newId = generatedKeys.getObject(primaryKey.getName());
						} else {
							newId = generatedKeys.getObject(1);
						}
						if (newId instanceof Long) {
							outId = (long) newId;
						} else if (newId instanceof Integer) {
							int newIdInt = (int) newId;
							outId = (long) newIdInt;
						}
					}
				}
			} catch (SQLException e) {
				if (errorItem != null) {
					logger.severe("Item:" + errorItem.getName() + " - SQLType:" + errorItem.getSQLType() + " - Type:" + errorItem.getType());
				}
				logger.warning("error on insert into " + table + ": " + e.getMessage());
				throw e;
			} finally {
				st.close();
			}
		} else {
			logger.fine("SQL = " + sql);
		}
		return outId;
	}

	public static String getTableName(Object bean) {
		Table t = bean.getClass().getAnnotation(Table.class);
		String tableName = getTableName(bean.getClass());
		if (t != null && !isEmpty(t.name())) {
			tableName = t.name();
		}
		return tableName;
	}

	public static String getTableName(Class clazz) {
		Table t = (Table) clazz.getAnnotation(Table.class);
		if (t == null) {
			return null;
		}
		String tableName;
		if (t.camelToSnake()) {
			tableName = StringHelperSql.camelToSnake(clazz.getSimpleName());
		} else {
			tableName = clazz.getSimpleName().toLowerCase();
		}
		if (!isEmpty(t.name())) {
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
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	/**
	 * Returns a parameterized WHERE clause using ? placeholders.
	 * Returns null if any WHERE value is null (treated as no filter).
	 */
	private static final String createWereClause(Collection<SQLItem> whereIds) {
		StringBuilder where = new StringBuilder(" WHERE ");
		boolean first = true;
		for (SQLItem i : whereIds) {
			if (i.getValue() == null) {
				return null;
			}
			if (!first) {
				where.append(" AND ");
			}
			where.append('"').append(i.getName()).append("\"=?");
			first = false;
		}
		return where.toString();
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
		String sql = "SELECT * FROM \"" + tableName + "\"" + createWereClause(whereItems);
		PreparedStatement st = conn.prepareStatement(sql);
		try {
			bindWhereParams(st, whereItems, 1);
			try (ResultSet rs = st.executeQuery()) {
				if (rs.next()) {
					rsToBean(rs, bean);
				} else {
					return null;
				}
				if (rs.next()) {
					throw new SQLException("more than one result on load bean : " + bean);
				}
				return bean;
			}
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
			throw new SQLException("no where clause defined in bean : " + bean.getClass().getCanonicalName());
		}
		update(conn, whereItems, bean, null);
		return true;
	}

	public static boolean insertOrUpdate(IConnectionProvider connProv, Object bean) throws SQLException {
		Connection conn = connProv.getConnection();
		try {
			try {
				return insertOrUpdate(conn, bean);
			} catch (SQLException e) {
				throw e;
			} catch (Exception e) {
				throw new SQLException(e);
			}
		} finally {
			connProv.releaseConnection(conn);
		}
	}

	public static boolean insertOrUpdate(Connection conn, Object bean) throws SQLException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		if (bean == null) {
			return false;
		}
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
		boolean update = false;
		String where = createWereClause(whereItems);
		if (where != null) {
			String sql = "SELECT \"" + whereItems.iterator().next().getName() + "\" FROM \"" + tableName + "\"" + where;
			if (DEBUG) {
				logger.fine("SQLBuilder.insertOrUpdate sql=" + sql);
			}
			PreparedStatement st = conn.prepareStatement(sql);
			try {
				bindWhereParams(st, whereItems, 1);
				try (ResultSet rs = st.executeQuery()) {
					if (rs.next()) {
						update = true;
					}
				}
			} finally {
				st.close();
			}
		}
		if (DEBUG) {
			logger.fine("SQLBuilder.insertOrUpdate update=" + update);
		}
		if (update) {
			update(conn, whereItems, bean, null);
			return false;
		} else {
			Long id = insert(conn, bean);
			if (DEBUG) {
				logger.fine("SQLBuilder.insertOrUpdate new id=" + id);
			}
			if (id != null) {
				String setMethod = null;
				Method getM = null;
				for (Method m : bean.getClass().getMethods()) {
					for (Annotation a : m.getAnnotations()) {
						if (a instanceof Column && ((Column) a).primaryKey()) {
							getM = m;
							setMethod = m.getName().replaceFirst("get", "set");
						}
					}
				}
				if (setMethod != null) {
					for (Method m : bean.getClass().getMethods()) {
						if (m.getName().equals(setMethod) && m.getParameters().length == 1 && m.getParameters()[0].getType().isAssignableFrom(getM.getReturnType())) {
							try {
								m.invoke(bean, id);
							} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
								logger.severe("setMethod=" + setMethod + " " + m + ": " + e.getMessage());
								throw e;
							}
						}
					}
				}
			}
			return true;
		}
	}

	public static Long insertOrUpdate(Connection conn, Collection<SQLItem> whereIds, String table, Collection<SQLItem> items, Collection<SQLItem> insertItem, Collection<SQLItem> updateItem) throws SQLException {
		return insertOrUpdate(conn, whereIds, table, items, insertItem, updateItem, true);
	}

	public static Long insertOrUpdate(Connection conn, Collection<SQLItem> whereIds, String table, Collection<SQLItem> items, Collection<SQLItem> insertItem, Collection<SQLItem> updateItem, boolean generateKey) throws SQLException {
		boolean update;
		String sql = "SELECT \"" + whereIds.iterator().next().getName() + "\" FROM \"" + table + "\"" + createWereClause(whereIds);
		PreparedStatement st = conn.prepareStatement(sql);
		try {
			bindWhereParams(st, whereIds, 1);
			try (ResultSet rs = st.executeQuery()) {
				update = rs.next();
			}
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

	public static boolean delete(IConnectionProvider connProv, Object bean) throws SQLException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Connection conn = connProv.getConnection();
		try {
			return delete(conn, bean);
		} finally {
			connProv.releaseConnection(conn);
		}
	}

	public static boolean delete(Connection conn, Object bean) throws SQLException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		return delete(conn, getTableName(bean), (long) getPrimaryKey(bean).getValue());
	}

	public static boolean delete(Connection conn, String table, long id) throws SQLException {
		if (conn != null) {
			PreparedStatement st = conn.prepareStatement("DELETE FROM \"" + table + "\" WHERE id=?");
			try {
				st.setLong(1, id);
				return st.execute();
			} catch (SQLException e) {
				logger.severe("error in delete from " + table + ": " + e.getMessage());
				throw e;
			} finally {
				st.close();
			}
		} else {
			logger.fine("DEBUG delete from " + table + " id=" + id);
		}
		return false;
	}

	public static Long update(Connection conn, Collection<SQLItem> whereIds, String table, Collection<SQLItem> items, boolean generateKey) throws SQLException {
		assert (whereIds.size() > 0);
		String sql = "UPDATE \"" + table + "\" SET ";
		Long outId = null;
		for (SQLItem i : items) {
			sql = sql + '"' + i.getName() + "\"=?,";
		}
		String whereClause = createWereClause(whereIds);
		if (whereClause == null) {
			throw new SQLException("UPDATE aborted: WHERE clause has null values (would affect all rows)");
		}
		sql = sql.substring(0, sql.length() - 1) + whereClause;
		SQLItem errorItem = null;
		if (conn != null) {
			PreparedStatement st = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			try {
				int i = 1;
				for (SQLItem item : items) {
					errorItem = item;
					bindParam(st, i++, item);
				}
				bindWhereParams(st, whereIds, i);
				errorItem = null;
				st.execute();
				if (generateKey) {
					try (ResultSet generatedKeys = st.getGeneratedKeys()) {
						if (generatedKeys.next()) {
							try {
								outId = generatedKeys.getLong(1);
							} catch (Exception e) {
								// some drivers don't return generated keys on UPDATE
							}
						}
					}
				}
			} catch (SQLException e) {
				if (errorItem != null) {
					logger.severe("Item:" + errorItem.getName() + " - SQLType:" + errorItem.getSQLType() + " - Type:" + errorItem.getType() + " - value:" + errorItem.getValue());
				}
				logger.severe("error in update " + table + ": " + e.getMessage());
				throw e;
			} finally {
				st.close();
			}
		} else {
			logger.fine("SQL = " + sql);
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
			return ((String) value).replace("'", "''");
		}
	}

	public static void createOrUpdateTable(Connection conn, Object bean) throws SQLException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		createOrUpdateTable(conn, bean, false);
	}

	public static void createOrUpdateTable(Connection conn, Object bean, boolean debug) throws SQLException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		String tableName = getTableName(bean);
		Statement st = conn.createStatement();
		Map<Integer, String> sqlTypes = getAllJdbcTypeNames();
		try {
			String sql = "CREATE TABLE \"" + tableName + "\" ()";
			st.execute(sql);
			if (debug) {
				logger.fine("create table: " + tableName);
			}
		} catch (SQLException e) {
			if (debug) {
				logger.fine("error create table: " + e.getMessage());
			}
		}
		try {
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
					if (debug) {
						logger.fine("error add column: " + e.getMessage());
					}
				}
				if (item.isPrimaryKey()) {
					sql = "ALTER TABLE \"" + tableName + "\" ADD PRIMARY KEY (\"" + item.getName() + "\")";
				}
				try {
					st.execute(sql);
				} catch (Exception e) {
					if (debug) {
						logger.fine("error primary key: " + e.getMessage());
					}
				}

				if (item.isNotNull()) {
					sql = "ALTER TABLE \"" + tableName + "\" ALTER COLUMN \"" + item.getName() + "\" SET NOT NULL";
				} else {
					sql = "ALTER TABLE \"" + tableName + "\" ALTER COLUMN \"" + item.getName() + "\" DROP NOT NULL";
				}
				try {
					st.execute(sql);
				} catch (Exception e) {
					if (debug) {
						logger.fine("error alter column: " + e.getMessage());
					}
				}

				String contrainName = "unique_" + item.getName();
				if (item.isUnique()) {
					sql = "ALTER TABLE \"" + tableName + "\" ADD CONSTRAINT " + contrainName + " UNIQUE (\"" + item.getName() + "\")";
				} else {
					sql = "ALTER TABLE \"" + tableName + "\" DROP CONSTRAINT \"" + contrainName + "\"";
				}
				try {
					st.execute(sql);
				} catch (Exception e) {
					if (debug) {
						logger.fine("error constraint: " + e.getMessage());
					}
				}

				if (!isEmpty(item.getDefaultValue())) {
					if (isNumeric(item.getType())) {
						sql = "ALTER TABLE \"" + tableName + "\" ALTER \"" + item.getName() + "\" SET DEFAULT " + item.getDefaultValue();
					} else {
						sql = "ALTER TABLE \"" + tableName + "\" ALTER \"" + item.getName() + "\" SET DEFAULT '" + escapeString(item.getDefaultValue(), false) + "'";
					}
					try {
						st.execute(sql);
					} catch (Exception e) {
						if (debug) {
							logger.fine("error alter default: " + e.getMessage());
						}
					}
				}

				if (!isEmpty(item.getForeign())) {
					if (!item.getForeign().contains(".")) {
						throw new SQLException("foreign format : #table#.#column#");
					}
					sql = "ALTER TABLE \"" + tableName + "\" ADD CONSTRAINT " + item.getName() + "_foreign FOREIGN KEY (\"" + item.getName() + "\") REFERENCES \"" + item.getForeign().split("\\.")[0] + "\" (" + item.getForeign().split("\\.")[1] + ") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE";
					if (debug) {
						logger.fine("add foreign key: " + sql);
					}
					try {
						st.execute(sql);
					} catch (Exception e) {
						if (debug) {
							logger.fine("error add foreign key: " + e.getMessage());
						}
					}
				}
			}
		} finally {
			st.close();
		}
	}

	public static String escapeString(String field, boolean escapeDoubleQuotes) {
		StringBuilder sBuilder = new StringBuilder(field.length() * 11 / 10);

		int stringLength = field.length();

		for (int i = 0; i < stringLength; ++i) {
			char c = field.charAt(i);

			switch (c) {
			case 0: /* Must be escaped for 'mysql' */
				sBuilder.append('\\');
				sBuilder.append('0');
				break;

			case '\n': /* Must be escaped for logs */
				sBuilder.append('\\');
				sBuilder.append('n');
				break;

			case '\r':
				sBuilder.append('\\');
				sBuilder.append('r');
				break;

			case '\\':
				sBuilder.append('\\');
				sBuilder.append('\\');
				break;

			case '\'':
				sBuilder.append('\'');
				sBuilder.append('\'');
				break;

			case '"': /* Better safe than sorry */
				if (escapeDoubleQuotes) {
					sBuilder.append('\\');
				}
				sBuilder.append('"');
				break;

			case '\032': /* This gives problems on Win32 */
				sBuilder.append('\\');
				sBuilder.append('Z');
				break;

			case '\u00a5':
			case '\u20a9':
				// escape characters interpreted as backslash by mysql
				// fall through

			default:
				sBuilder.append(c);
			}
		}

		return sBuilder.toString();
	}

	public static void main(String[] args) {
		System.out.println("escape = " + escapeString("patrick 'est' vraiment beau.", true));
	}

	private static boolean isNumeric(String type) {
		if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("long") || type.equalsIgnoreCase("real")) {
			return true;
		} else {
			return false;
		}
	}
}
