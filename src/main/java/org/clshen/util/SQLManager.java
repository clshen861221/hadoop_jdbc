package org.clshen.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

public class SQLManager {
	private static final Logger log = Logger.getLogger(SQLManager.class);

	public static List<Object> searchEntity(String sSQL, List<Object> params,
			Connection oSQLConnection, Class<?> clazz) throws Exception {
		DatabaseMetaData metaData = oSQLConnection.getMetaData();
		String databaseName = metaData.getDatabaseProductName();
		if ("MySQL".equals(databaseName) || "Apache Hive".equals(databaseName)) {
			sSQL = sSQL.toLowerCase();
		} else {
			sSQL = sSQL.toUpperCase();
		}
		List<Object> objects = new ArrayList<Object>();

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {
			ps = oSQLConnection.prepareStatement(sSQL);

			if (params != null && params.size() > 0) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
			}

			rs = ps.executeQuery();

			while (rs.next()) {

				Object obj = clazz.newInstance();

				ResultSetMetaData rsm = rs.getMetaData();

				for (int i = 0; i < rsm.getColumnCount(); i++) {
					String columnName = rsm.getColumnName(i + 1);

					Field[] fields = clazz.getDeclaredFields();

					for (int j = 0; j < fields.length; j++) {
						String fieldName = fields[j].getName();
						if (fieldName.toUpperCase().equals(
								columnName.toUpperCase())) {
							Method method = null;
							try {
								method = clazz.getMethod(
										"set"
												+ fieldName.substring(0, 1)
														.toUpperCase()
												+ fieldName.substring(1),
										fields[j].getType());

								method.invoke(obj, rs.getObject(columnName));
							} catch (Exception e) {
								method = null;
							}

							break;
						}
					}
				}

				objects.add(obj);

			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				try {
					rs.close();
					rs = null;
				} catch (SQLException e) {
					throw e;
				}
			}

			if (ps != null) {
				try {
					ps.close();
					ps = null;
				} catch (SQLException e) {
					throw e;
				}
			}
		}

		return objects;
	}

	public static void executeSql(String sSQL, Connection oSQLConnection,
			List<Object> params) throws Exception {
		PreparedStatement ps = null;

		DatabaseMetaData metaData = oSQLConnection.getMetaData();
		String databaseName = metaData.getDatabaseProductName();
		if ("MySQL".equals(databaseName)) {
			sSQL = sSQL.toLowerCase();
		} else {
			sSQL = sSQL.toUpperCase();
		}

		try {
			ps = oSQLConnection.prepareStatement(sSQL);

			if (params != null && params.size() > 0) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
			}

			ps.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {

			if (ps != null) {
				try {
					ps.close();
					ps = null;
				} catch (SQLException e) {
					e.printStackTrace();
					throw e;
				}
			}
		}

	}

	public static List<List<Object>> executeQuery(String sSQL,
			Connection oSQLConnection, List<Object> params) throws Exception {
		DatabaseMetaData databaseMetaData = oSQLConnection.getMetaData();
		String databaseName = databaseMetaData.getDatabaseProductName();
		if ("MySQL".equals(databaseName)) {
			sSQL = sSQL.toLowerCase();
		} else {
			sSQL = sSQL.toUpperCase();
		}
		List<List<Object>> records = new ArrayList<List<Object>>();

		List<Object> columns = new ArrayList<Object>();

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			ps = oSQLConnection.prepareStatement(sSQL);

			if (params != null && params.size() > 0) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
			}

			rs = ps.executeQuery();

			ResultSetMetaData metaData = rs.getMetaData();

			Integer colNum = metaData.getColumnCount();

			while (rs.next()) {
				columns.clear();
				for (int i = 1; i <= colNum; i++) {
					columns.add(rs.getObject(i));
				}
				records.add(columns);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				try {
					rs.close();
					rs = null;
				} catch (SQLException e) {
					throw e;
				}
			}

			if (ps != null) {
				try {
					ps.close();
					ps = null;
				} catch (SQLException e) {
					throw e;
				}
			}
		}

		return records;
	}

	public static String tableColumnTool(String tableName, Connection conn)
			throws Exception {
		String sReturn = "";

		String sql = "SELECT * FROM " + tableName + " WHERE 1=2";

		Statement st = null;

		ResultSet rs = null;

		DatabaseMetaData databaseMetaData = conn.getMetaData();
		String databaseName = databaseMetaData.getDatabaseProductName();
		if ("MySQL".equals(databaseName)) {
			sql = sql.toLowerCase();
		} else {
			sql = sql.toUpperCase();
		}

		try {
			st = conn.createStatement();

			rs = st.executeQuery(sql);

			ResultSetMetaData rsmd = rs.getMetaData();

			int columnNum = rsmd.getColumnCount();

			for (int i = 1; i <= columnNum; i++) {
				String columnName = rsmd.getColumnName(i).toLowerCase();

				String columnCatalog = rsmd.getColumnClassName(i);

				String columnTCatalogName = columnCatalog
						.substring(columnCatalog.lastIndexOf(".") + 1);

				sReturn = sReturn + "private " + columnTCatalogName + " "
						+ columnName + "; \n \n";

			}

		} catch (Exception e) {
			e.printStackTrace();

			throw e;
		} finally {
			if (rs != null) {
				rs.close();
				rs = null;
			}

			if (st != null) {
				st.close();
				st = null;
			}
		}

		return sReturn;
	}

	public static void saveEntity(Object entity, String tableName,
			Connection conn) throws Exception {

		if (entity == null || tableName == null || "".equals(tableName.trim())) {
			return;
		}

		List<String> fieldNameList = new ArrayList<String>();

		List<Object> fieldValueList = new ArrayList<Object>();

		Class<?> clazz = entity.getClass();

		tableName = tableName.trim().toUpperCase();

		String insertSql = "INSERT INTO " + tableName + "(";

		PreparedStatement ps = null;

		try {

			Field[] fields = clazz.getDeclaredFields();

			Object fieldValue = null;

			Method method = null;

			for (int j = 0; j < fields.length; j++) {
				String fieldName = fields[j].getName();

				try {
					method = clazz.getMethod("get"
							+ fieldName.substring(0, 1).toUpperCase()
							+ fieldName.substring(1));

					fieldValue = method.invoke(entity);

				} catch (Exception e) {
					method = null;
				}

				if (method == null || fieldValue == null
						|| !isBasicType(fieldValue.getClass())) {
					continue;
				}

				if (fieldValue instanceof Boolean) {
					Boolean bol = (Boolean) fieldValue;

					if (bol) {
						fieldValue = 1;
					} else {
						fieldValue = 0;
					}
				}

				fieldNameList.add(fieldName);

				fieldValueList.add(fieldValue);

			}

			if (fieldNameList == null || fieldNameList.size() == 0) {
				insertSql = null;
			} else {
				for (String fieldName : fieldNameList) {
					insertSql = insertSql + fieldName + ",";
				}

				insertSql = insertSql.substring(0, insertSql.length() - 1)
						+ ") VALUES(";

				for (int i = 0; i < fieldValueList.size(); i++) {
					insertSql = insertSql + "?,";
				}

				insertSql = insertSql.substring(0, insertSql.length() - 1)
						+ ")";

				DatabaseMetaData metaData = conn.getMetaData();
				String databaseName = metaData.getDatabaseProductName();
				if ("MySQL".equals(databaseName)) {
					insertSql = insertSql.toLowerCase();
				} else {
					insertSql = insertSql.toUpperCase();
				}

				ps = conn.prepareStatement(insertSql);

				// System.out.println(insertSql);

				for (int i = 0; i < fieldValueList.size(); i++) {
					Object value = fieldValueList.get(i);

					ps.setObject(i + 1, value);
				}

				ps.executeUpdate();

			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}
		}

	}

	public static Object updateEntity(Object entity, String tableName,
			Connection conn) throws Exception {
		Object obj = null;

		if (entity == null || tableName == null || "".equals(tableName.trim())) {
			return null;
		}

		List<String> fieldNameList = new ArrayList<String>();

		List<Object> fieldValueList = new ArrayList<Object>();

		List<Object> updatedFieldValueList = new ArrayList<Object>();

		Class<?> clazz = entity.getClass();

		String updateSql = "UPDATE " + tableName + " SET ";

		ResultSet pkRs = null;

		PreparedStatement ps = null;

		String primaryKeyColumnName = null;

		Object primaryKeyValue = null;
		DatabaseMetaData dmd = conn.getMetaData();
		String databaseName = dmd.getDatabaseProductName();
		if ("MySQL".equals(databaseName)) {
			tableName = tableName.trim().toLowerCase();
		} else {
			tableName = tableName.trim().toUpperCase();
		}

		try {
			pkRs = dmd.getPrimaryKeys(null, null, tableName);

			while (pkRs.next()) {
				primaryKeyColumnName = pkRs.getString("COLUMN_NAME");
			}

			if (primaryKeyColumnName != null) {
				Field[] fields = clazz.getDeclaredFields();
				Method method = null;
				Object fieldValue = null;
				for (int j = 0; j < fields.length; j++) {
					String fieldName = fields[j].getName();

					try {
						method = clazz.getMethod("get"
								+ fieldName.substring(0, 1).toUpperCase()
								+ fieldName.substring(1));

						fieldValue = method.invoke(entity);

					} catch (Exception e) {
						method = null;
					}

					if (method == null) {
						continue;
					}

					if (fieldName.toUpperCase().equals(
							primaryKeyColumnName.toUpperCase())) {
						primaryKeyValue = fieldValue;

						continue;
					}

					if (!isBasicType(fields[j].getType())) {
						continue;
					}

					if (fieldValue instanceof Boolean) {
						Boolean bol = (Boolean) fieldValue;

						if (bol) {
							fieldValue = 1;
						} else {
							fieldValue = 0;
						}
					}

					fieldNameList.add(fieldName);

					fieldValueList.add(fieldValue);

				}

				if (fieldNameList == null || fieldNameList.size() == 0) {
					updateSql = null;
				} else {
					for (int i = 0; i < fieldNameList.size(); i++) {
						String columName = fieldNameList.get(i);

						Object value = fieldValueList.get(i);

						if (value == null) {
							updateSql = updateSql + columName + "=null, ";

						} else {
							updateSql = updateSql + columName + "=?, ";
						}
					}

					updateSql = updateSql.substring(0, updateSql.length() - 2)
							+ " WHERE " + primaryKeyColumnName + "=?";

					if ("MySQL".equals(databaseName)) {
						updateSql = updateSql.toLowerCase();
					} else {
						updateSql = updateSql.toUpperCase();
					}

					// System.out.println(updateSql);

					ps = conn.prepareStatement(updateSql);

					if (primaryKeyValue == null) {
						return null;
					} else {
						fieldValueList.add(primaryKeyValue);

						for (int i = 0; i < fieldValueList.size(); i++) {
							Object o = fieldValueList.get(i);

							if (o != null) {
								updatedFieldValueList.add(o);
							}
						}

						for (int i = 0; i < updatedFieldValueList.size(); i++) {
							Object value = updatedFieldValueList.get(i);

							ps.setObject(i + 1, value);

						}

						ps.executeUpdate();

						obj = findEntityByIdentity(primaryKeyValue, tableName,
								clazz, conn);
					}

				}

				return obj;

			} else {
				return null;
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}

			if (pkRs != null) {
				pkRs.close();
				pkRs = null;
			}
		}
	}

	public static Object findEntityByIdentity(Object id, String tableName,
			Class<?> clazz, Connection conn) throws Exception {
		DatabaseMetaData metaData = conn.getMetaData();
		String databaseName = metaData.getDatabaseProductName();
		Object obj = null;

		if (id == null) {
			return obj;
		}

		if ("MySQL".equals(databaseName)) {
			tableName = tableName.trim().toLowerCase();
		} else {
			tableName = tableName.trim().toUpperCase();
		}

		String getEntitySql = "SELECT * FROM " + tableName + " WHERE ";

		ResultSet pkRs = null;

		PreparedStatement entityPs = null;

		ResultSet entityRs = null;

		String primaryKeyColumnName = null;

		try {

			DatabaseMetaData dmd = conn.getMetaData();

			pkRs = dmd.getPrimaryKeys(null, null, tableName);

			while (pkRs.next()) {
				primaryKeyColumnName = pkRs.getString("COLUMN_NAME");
			}

			if (primaryKeyColumnName != null) {
				getEntitySql = getEntitySql + primaryKeyColumnName + "=?";

				if ("MySQL".equals(databaseName)) {
					getEntitySql = getEntitySql.toLowerCase();
				} else {
					getEntitySql = getEntitySql.toUpperCase();
				}

				// System.out.println(getEntitySql);

				entityPs = conn.prepareStatement(getEntitySql);

				entityPs.setObject(1, id);

				entityRs = entityPs.executeQuery();

				while (entityRs.next()) {
					obj = clazz.newInstance();

					ResultSetMetaData rsm = entityRs.getMetaData();

					for (int i = 0; i < rsm.getColumnCount(); i++) {
						String columnName = rsm.getColumnName(i + 1);

						Field[] fields = clazz.getDeclaredFields();

						for (int j = 0; j < fields.length; j++) {
							String fieldName = fields[j].getName();
							if (fieldName.toUpperCase().equals(
									columnName.toUpperCase())) {
								Method method = null;

								try {
									method = clazz.getMethod(
											"set"
													+ fieldName.substring(0, 1)
															.toUpperCase()
													+ fieldName.substring(1),
											fields[j].getType());

									method.invoke(obj,
											entityRs.getObject(columnName));
								} catch (Exception e) {
									method = null;
								}

								break;
							}
						}
					}
				}
			}

		} catch (Exception e) {
			throw e;
		} finally {
			if (entityRs != null) {
				entityRs.close();
				entityRs = null;
			}

			if (entityPs != null) {
				entityPs.close();
				entityPs = null;
			}

			if (pkRs != null) {
				pkRs.close();
				pkRs = null;
			}
		}

		return obj;
	}

	public static void insertBatch(Connection conn, List<?> list,
			String identityColumnName, boolean isAutoIdentity, String tableName)
			throws Exception {
		if (list == null || list.size() == 0) {
			return;
		}

		Object obj = list.get(0);
		Class<?> clazz = obj.getClass();
		Field[] fields = clazz.getDeclaredFields();

		List<String> replaceList = new ArrayList<String>();
		StringBuffer sql = new StringBuffer("INSERT INTO " + tableName + " (");
		Field field = null;
		String fieldName = null;
		Object fieldValue = null;
		Method method = null;
		List<Field> fieldList = new ArrayList<Field>();
		for (int i = 0; i < fields.length; i++) {
			field = fields[i];
			if (isBasicType(field.getType())) {
				fieldList.add(field);
			}
		}

		for (int i = 0; i < fieldList.size(); i++) {
			field = fieldList.get(i);
			fieldName = field.getName();

			try {
				method = clazz.getMethod("get"
						+ fieldName.substring(0, 1).toUpperCase()
						+ fieldName.substring(1));
			} catch (Exception e) {
				method = null;
			}

			if (method == null) {
				continue;
			}

			if (isAutoIdentity) {
				if (identityColumnName != null
						&& field.getName().toUpperCase()
								.equals(identityColumnName.toUpperCase())) {
					continue;
				}
			}

			if (!isBasicType(field.getType())) {
				continue;
			}
			if (i == fieldList.size() - 1) {
				sql.append(field.getName() + ") VALUES (");
				replaceList.add("?)");
			} else {
				sql.append(field.getName() + ",");
				replaceList.add("?,");
			}

		}
		for (int i = 0; i < replaceList.size(); i++) {
			sql.append(replaceList.get(i));
		}

		DatabaseMetaData metaData = conn.getMetaData();
		String databaseName = metaData.getDatabaseProductName();
		if ("MySQL".equals(databaseName)) {
			sql = new StringBuffer(sql.toString().toLowerCase());
		} else {
			sql = new StringBuffer(sql.toString().toUpperCase());
		}

		// System.out.println("Batch Insert:" + sql.toString());

		PreparedStatement ps = null;
		Class<?> type = null;
		int index = 0;
		try {
			ps = conn.prepareStatement(sql.toString());

			for (Object bean : list) {
				for (int i = 0; i < fieldList.size(); i++) {
					field = fieldList.get(i);
					fieldName = field.getName();
					if (isAutoIdentity) {
						if (identityColumnName != null
								&& fieldName.toUpperCase().equals(
										identityColumnName.toUpperCase())) {
							continue;
						}
					}

					try {
						method = clazz.getMethod("get"
								+ fieldName.substring(0, 1).toUpperCase()
								+ fieldName.substring(1));
						fieldValue = method.invoke(bean);
					} catch (Exception e) {
						method = null;
					}

					if (method == null) {
						continue;
					}

					type = field.getType();

					if (fieldValue == null) {
						ps.setNull(index + 1, getJDBCType(type));
					} else {
						ps.setObject(index + 1, fieldValue);
					}

					index++;
				}

				index = 0;

				ps.addBatch();
			}
			ps.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}
		}

	}

	public static void updateBatch(Connection conn, List<?> list,
			String tableName) throws Exception {
		if (list == null || list.size() == 0 || tableName == null
				|| "".equals(tableName.trim())) {
			return;
		}
		Object entity = list.get(0);
		List<String> fieldNameList = new ArrayList<String>();
		List<Object> fieldValueList = new ArrayList<Object>();
		Class<?> clazz = entity.getClass();
		tableName = tableName.trim().toUpperCase();
		ResultSet pkRs = null;
		String primaryKeyColumnName = null;
		Object primaryKeyValue = null;
		PreparedStatement ps = null;
		Field field = null;
		String fieldName = null;
		Method method = null;
		Object fieldValue = null;
		Class<?> type = null;
		try {
			DatabaseMetaData dmd = conn.getMetaData();
			pkRs = dmd.getPrimaryKeys(null, null, tableName);

			while (pkRs.next()) {
				primaryKeyColumnName = pkRs.getString("COLUMN_NAME");
			}

			Field[] fields = clazz.getDeclaredFields();

			for (int j = 0; j < fields.length; j++) {
				field = fields[j];
				fieldName = field.getName();
				type = field.getType();
				if (!isBasicType(type)) {
					continue;
				}

				if (fieldName.toUpperCase().equals(
						primaryKeyColumnName.toUpperCase())) {
					continue;
				}
				fieldNameList.add(fieldName);

			}

			StringBuffer updateSql = new StringBuffer("UPDATE " + tableName
					+ " SET ");
			if (fieldNameList == null || fieldNameList.size() == 0) {
				updateSql = null;
			} else {
				for (int i = 0; i < fieldNameList.size(); i++) {
					String columName = fieldNameList.get(i);
					updateSql.append(columName + "=?, ");

				}

				updateSql = new StringBuffer(updateSql.toString().substring(0,
						updateSql.length() - 2));
				updateSql.append(" WHERE " + primaryKeyColumnName + "=?");

				DatabaseMetaData metaData = conn.getMetaData();
				String databaseName = metaData.getDatabaseProductName();
				if ("MySQL".equals(databaseName)) {
					updateSql = new StringBuffer(updateSql.toString()
							.toLowerCase());
				} else {
					updateSql = new StringBuffer(updateSql.toString()
							.toUpperCase());
				}
				// System.out.println("Batch update:" + updateSql.toString());

			}

			if (updateSql != null) {
				ps = conn.prepareStatement(updateSql.toString());
				for (Object obj : list) {
					fieldValueList.clear();
					fieldValueList = new ArrayList<Object>();
					for (int i = 0; i < fields.length; i++) {
						field = fields[i];
						fieldName = field.getName();
						type = field.getType();
						try {
							method = clazz.getMethod("get"
									+ fieldName.substring(0, 1).toUpperCase()
									+ fieldName.substring(1));
							fieldValue = method.invoke(obj);

							if (fieldName.toUpperCase().equals(
									primaryKeyColumnName.toUpperCase())) {
								primaryKeyValue = fieldValue;
								continue;
							}

							if (fieldValue == null) {
								Empty empty = new Empty();
								empty.setJdbcType(getJDBCType(type));
								fieldValue = empty;
							}

							if (fieldValue instanceof Boolean) {
								Boolean bol = (Boolean) fieldValue;
								if (bol) {
									fieldValue = 1;
								} else {
									fieldValue = 0;
								}
							}

							fieldValueList.add(fieldValue);
						} catch (Exception e) {
							method = null;
						}
					}

					if (primaryKeyValue == null) {
						return;
					} else {
						fieldValueList.add(primaryKeyValue);

						for (int i = 0; i < fieldValueList.size(); i++) {
							Object value = fieldValueList.get(i);
							if (value instanceof Empty) {
								Empty empty = (Empty) value;
								ps.setNull(i + 1, empty.getJdbcType());
							} else {
								ps.setObject(i + 1, value);
							}

						}

						ps.addBatch();

					}

				}

				ps.executeBatch();
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}
			if (pkRs != null) {
				pkRs.close();
				pkRs = null;
			}
		}

	}

	private static Integer getJDBCType(Class<?> clazz) {
		Integer type = null;

		if (clazz.equals(Long.class)) {
			type = Types.BIGINT;
		} else if (clazz.equals(Byte.class)) {
			type = Types.BINARY;
		} else if (clazz.equals(Boolean.class)) {
			type = Types.BIT;
		} else if (clazz.equals(String.class)) {
			type = Types.CHAR;
		} else if (clazz.equals(Date.class)) {
			type = Types.DATE;
		} else if (clazz.equals(BigDecimal.class)) {
			type = Types.DECIMAL;
		} else if (clazz.equals(Double.class)) {
			type = Types.FLOAT;
		} else if (clazz.equals(Integer.class)) {
			type = Types.INTEGER;
		} else if (clazz.equals(Short.class)) {
			type = Types.SMALLINT;
		} else if (clazz.equals(Timestamp.class)) {
			type = Types.TIMESTAMP;
		}

		return type;
	}

	public static boolean isBasicType(Class<?> clazz) {
		boolean isBasicType = false;
		if (clazz.equals(Long.class)) {
			isBasicType = true;
		} else if (clazz.equals(Byte.class)) {
			isBasicType = true;
		} else if (clazz.equals(Boolean.class)) {
			isBasicType = true;
		} else if (clazz.equals(String.class)) {
			isBasicType = true;
		} else if (clazz.equals(Date.class)) {
			isBasicType = true;
		} else if (clazz.equals(BigDecimal.class)) {
			isBasicType = true;
		} else if (clazz.equals(Double.class)) {
			isBasicType = true;
		} else if (clazz.equals(Integer.class)) {
			isBasicType = true;
		} else if (clazz.equals(Short.class)) {
			isBasicType = true;
		} else if (clazz.equals(Timestamp.class)) {
			isBasicType = true;
		}

		return isBasicType;

	}

	private static class Empty {
		private int jdbcType;

		public int getJdbcType() {
			return jdbcType;
		}

		public void setJdbcType(int jdbcType) {
			this.jdbcType = jdbcType;
		}
	}

	private static String BUNDLE_NAME = null;
	private static ResourceBundle RESOURCE_BUNDLE = null;
	static {
		Map<String, String> envMap = System.getenv();
		String deploy_scenario = envMap.get("DEPLOY_SCENARIO");
		if (deploy_scenario == null) {
			deploy_scenario = System.getProperty("DEPLOY_SCENARIO");
		}
		if (deploy_scenario == null) {
			deploy_scenario = "LOCAL";
		}
		log.info("============================DEPLOY_SCENARIO="
				+ deploy_scenario + "=============================");
		BUNDLE_NAME = "env/" + deploy_scenario + "/db";
		RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);
	}

	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}

	private static BasicDataSource OIMPALA_DATASOURCE = null;

	public static Connection getImpalaConnection() throws SQLException,
			ClassNotFoundException {
		Connection conn = null;
		if (RESOURCE_BUNDLE != null) {
			String driverClass = getString("hadoop.jdbc.impala.db.driver");
			String url = getString("hadoop.jdbc.impala.db.url");
			String username = getString("hadoop.jdbc.impala.db.username");
			String password = getString("hadoop.jdbc.impala.db.password");
			if (OIMPALA_DATASOURCE == null) {
				Class.forName(driverClass);
				OIMPALA_DATASOURCE = new BasicDataSource();
				OIMPALA_DATASOURCE.setDriverClassName(driverClass);
				OIMPALA_DATASOURCE.setUsername(username);
				OIMPALA_DATASOURCE.setPassword(password);
				OIMPALA_DATASOURCE.setUrl(url);
				OIMPALA_DATASOURCE.setMaxActive(200);
				OIMPALA_DATASOURCE.setMaxIdle(10);
				OIMPALA_DATASOURCE.setMaxWait(60000);
				OIMPALA_DATASOURCE.setTestOnBorrow(true);
				OIMPALA_DATASOURCE.setRemoveAbandoned(true);
				OIMPALA_DATASOURCE.setRemoveAbandonedTimeout(60);
				OIMPALA_DATASOURCE.setLogAbandoned(true);

			}

			conn = OIMPALA_DATASOURCE.getConnection();
		}
		return conn;
	}

	private static BasicDataSource oHIVE_DATASOURCE = null;

	public static Connection getHiveConnection()
			throws SQLException, ClassNotFoundException {
		Connection conn = null;
		if (RESOURCE_BUNDLE != null) {
			String driverClass = getString("hadoop.jdbc.hive.db.driver");
			String url = getString("hadoop.jdbc.hive.db.url");
			String username = getString("hadoop.jdbc.hive.db.username");
			String password = getString("hadoop.jdbc.hive.db.password");
			if (OIMPALA_DATASOURCE == null) {
				Class.forName(driverClass);
				oHIVE_DATASOURCE = new BasicDataSource();
				oHIVE_DATASOURCE.setDriverClassName(driverClass);
				oHIVE_DATASOURCE.setUsername(username);
				oHIVE_DATASOURCE.setPassword(password);
				oHIVE_DATASOURCE.setUrl(url);
				oHIVE_DATASOURCE.setMaxActive(200);
				oHIVE_DATASOURCE.setMaxIdle(10);
				oHIVE_DATASOURCE.setMaxWait(60000);
				oHIVE_DATASOURCE.setTestOnBorrow(true);
				oHIVE_DATASOURCE.setRemoveAbandoned(true);
				oHIVE_DATASOURCE.setRemoveAbandonedTimeout(60);
				oHIVE_DATASOURCE.setLogAbandoned(true);

			}

			conn = oHIVE_DATASOURCE.getConnection();
		}
		return conn;
	}

	public static void main(String[] args) throws Exception {
		Connection conn1 = SQLManager.getImpalaConnection();
		String result1 = SQLManager.tableColumnTool("Hourse_info", conn1);
		System.out.println(result1);
	}

}
