package com.adanac.framework.dac.datasource.group;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * 分组数据源类型处理类
 * @author adanac
 * @version 1.0
 */
public class DataSourceAware implements DataSource {
	public PrintWriter getLogWriter() throws SQLException {
		throw new UnsupportedOperationException("getLogWriter");
	}

	public void setLogWriter(PrintWriter out) throws SQLException {
		throw new UnsupportedOperationException("setLogWriter");
	}

	public void setLoginTimeout(int seconds) throws SQLException {
		throw new UnsupportedOperationException("setLoginTimeout");
	}

	public int getLoginTimeout() throws SQLException {
		return 0;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException("unwrap");
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException("isWrapperFor");
	}

	public Connection getConnection() throws SQLException {
		throw new UnsupportedOperationException("getConnection");
	}

	public Connection getConnection(String username, String password) throws SQLException {
		throw new UnsupportedOperationException("getConnection(username,password)");
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new UnsupportedOperationException("getParentLogger");
	}
}
