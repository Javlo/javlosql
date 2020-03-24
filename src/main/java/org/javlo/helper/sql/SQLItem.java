package org.javlo.helper.sql;

import java.sql.Types;
import java.util.Calendar;
import java.util.Date;

public class SQLItem {

	private String name;
	private String type;
	private Object value;
	private boolean primaryKey = false;
	private String foreign = "";
	private boolean notNull = false;
	private boolean auto = false;
	private String defaultValue = null;
	
	public SQLItem(String name, String type, Object value, boolean primaryKey, String foreign,boolean notNull,boolean auto, String defaultValue) {
		this.name = name;
		this.type = type;
		this.value = value;
		this.primaryKey = primaryKey;
		this.foreign = foreign;
		this.notNull = notNull;
		this.auto=auto;
		this.defaultValue = defaultValue;
	}

	public SQLItem(String name, String type, Object value) {
		super();
		this.name = name;
		this.type = type;
		this.value = value;
	}

	public SQLItem(String name, Object value) {
		super();
		this.name = name;
		if (value == null) {
			type = "string";
		} else {
			this.type = value.getClass().getSimpleName();
		}
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getSQLType() {
		if (type.equals("String")) {
			return Types.VARCHAR;
		} else if (type.equals("Date")) {
			return Types.DATE;
		} else if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("integer")) {
			return Types.INTEGER;
		} else if (type.equalsIgnoreCase("long")) {
			return Types.BIGINT;
		} else if (type.equals("Date")) {
			return Types.DATE;
		} else if (type.equalsIgnoreCase("time")) {
			return Types.TIME;
		} else if (type.equalsIgnoreCase("timestamp")) {
			return Types.TIMESTAMP;
		} else if (type.equalsIgnoreCase("double")) {
			return Types.REAL;
		} else {
			return Types.VARCHAR;
		}
	}

	public Object getValue() {
		if (value == null) {
			return null;
		}
		if (value instanceof Long) {
			return ((Long) value).longValue();
		}
		if (value instanceof Integer) {
			return ((Integer) value).intValue();
		}
		if (value instanceof java.util.Date) {
			if (type.equalsIgnoreCase("timestamp")) {
				return new java.sql.Timestamp(((Date) value).getTime());
			} else if (type.equalsIgnoreCase("time")) {
				return new java.sql.Time(((Date) value).getTime());
			} else {
				return new java.sql.Date(((Date) value).getTime());
			}
		}
		if (value instanceof java.util.Calendar) {
			if (type.equalsIgnoreCase("timestamp")) {
				return new java.sql.Timestamp(((Calendar) value).getTimeInMillis());
			} else if (type.equalsIgnoreCase("time")) {
				return new java.sql.Time(((Calendar) value).getTimeInMillis());
			} else {
				return new java.sql.Date(((Calendar) value).getTimeInMillis());
			}
		}
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public boolean isPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(boolean primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getForeign() {
		return foreign;
	}

	public void setForeign(String foreign) {
		this.foreign = foreign;
	}

	public boolean isNotNull() {
		return notNull;
	}

	public void setNotNull(boolean notNull) {
		this.notNull = notNull;
	}

	public boolean isAuto() {
		return auto;
	}

	public void setAuto(boolean auto) {
		this.auto = auto;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

}
