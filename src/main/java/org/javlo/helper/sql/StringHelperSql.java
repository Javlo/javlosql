package org.javlo.helper.sql;

public class StringHelperSql {

	public static String camelToSnake(final String camelStr) {
		String ret = camelStr.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z])([A-Z])", "$1_$2");
		return ret.toLowerCase();
	}

	public static boolean isEmpty(String name) {
		if (name == null) {
			return true;
		} else {
			return name.trim().length() == 0;
		}
	}

}
