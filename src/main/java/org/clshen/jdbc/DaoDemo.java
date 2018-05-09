package org.clshen.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.clshen.entity.Hourse_info;
import org.clshen.util.SQLManager;

public class DaoDemo {

	public static void main(String[] args) {
		// hiveJdbcDemo();
		hivePoolDemo();
		// impalaJdbcDemo();
		// impalaPoolDemo();
	}

	public static void hiveJdbcDemo() {
		String driverName = "org.apache.hive.jdbc.HiveDriver";
		String url = "jdbc:hive2://quickstart.cloudera:10000/default";

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			String sql = "select * from hourse_info limit 10";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
					rs = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
					stmt = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
					conn = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static void hivePoolDemo() {
		Connection conn = null;
		try {
			conn = SQLManager.getHiveConnection();
			String sql = "select * from hourse_info where name = '沈辉' limit 10";
			List<Object> list = SQLManager.searchEntity(sql, null, conn,
					Hourse_info.class);
			for (Object obj : list) {
				System.out.println(obj);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public static void impalaJdbcDemo() {
		String driver = "com.cloudera.impala.jdbc4.Driver";
		String url = "jdbc:impala://quickstart.cloudera:21050/default";
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement pst = null;

		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url);
			pst = conn.prepareStatement("select * from hourse_info limit 10");
			rs = pst.executeQuery();
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
				pst.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public static void impalaPoolDemo() {
		Connection conn = null;

		try {
			conn = SQLManager.getImpalaConnection();
			String sql = "select * from hourse_info where name = '沈辉' limit 10";
			List<Object> list = SQLManager.searchEntity(sql, null, conn,
					Hourse_info.class);
			for (Object obj : list) {
				System.out.println(obj);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
