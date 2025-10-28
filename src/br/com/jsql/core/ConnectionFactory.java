package br.com.jsql.core;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConnectionFactory {
	private final Connection connection;
	private PreparedStatement statement;
	private ResultSet rs;
	private String sql;

	public ConnectionFactory(String propertyFile) {
		sql = "";
		try {
			final Properties properties = getPropertiesFile(propertyFile);

			if (propertyFile == null) {
				throw new NullPointerException("Properties is null. You need add not null properties file.");
			}

			final String url = properties.getProperty("jdbc.url");
			final String user = properties.getProperty("jdbc.user");
			final String password = properties.getProperty("jdbc.password");
			connection = DriverManager.getConnection(url, user, password);
		} catch (SQLException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	private Properties getPropertiesFile(String propertyFile) throws IOException {
		Properties properties = new Properties();
		properties.load(ConnectionFactory.class.getResourceAsStream(propertyFile));
		return properties;
	}

	public ConnectionFactory insertInto(String table, String... fields) {
		sql = "INSERT INTO " + table + " (";

		for (String field : fields) {
			sql += field + ", ";
		}

		sql += ")";

		if (sql.contains(", )")) {
			sql = sql.replace(", )", ")");
		}

		return this;
	}

	public ConnectionFactory values(Object... objects) {
		sql += " VALUES (";

		if (objects == null) {
			throw new NullPointerException("Values is null. You need add not null value.");
		}

		for (int i = 0; i < objects.length; i++) {
			sql += "?, ";
		}

		sql += ")";

		if (sql.contains(", )")) {
			sql = sql.replace(", )", ")");
		}

		sql += ";";

		try {
			statement = connection.prepareStatement(sql);

			int i = 1;

			for (Object object : objects) {
				if (object instanceof String) {
					statement.setString(i, (String) object);
				}

				if (object instanceof Integer) {
					statement.setInt(i, (Integer) object);
				}

				if (object instanceof Double) {
					statement.setDouble(i, (Double) object);
				}

				if (object instanceof Boolean) {
					statement.setBoolean(i, (Boolean) object);
				}
			}

			statement.execute();
			statement.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		return this;
	}

	public ConnectionFactory selectFrom(String table) {
		sql = "";
		sql = "SELECT * FROM " + table;
		return this;
	}

	public Map<String, List<Object>> whereGet(String field, String operator, Object value) {
		sql += " WHERE " + field + " " + operator + " ? ";
		try {
			statement = connection.prepareStatement(sql);

			if (value instanceof String) {
				statement.setString(1, (String) value);
			}

			if (value instanceof Integer) {
				statement.setInt(1, (Integer) value);
			}

			if (value instanceof Double) {
				statement.setDouble(1, (Double) value);
			}

			if (value instanceof Boolean) {
				statement.setBoolean(1, (Boolean) value);
			}

			return this.get();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Map<String, List<Object>> get() throws SQLException {
		if (statement == null) {
			sql += ";";
			statement = connection.prepareStatement(sql);
		}

		if (statement != null) {
			rs = statement.executeQuery();
		}

		ResultSetMetaData md = rs.getMetaData();

		int columns = md.getColumnCount();
		Map<String, List<Object>> map = new HashMap<>(columns);

		for (int i = 1; i <= columns; ++i) {
			map.put(md.getColumnName(i), new ArrayList<>());
		}

		while (rs.next()) {
			for (int i = 1; i <= columns; ++i) {
				map.get(md.getColumnName(i)).add(rs.getObject(i));
			}
		}

		return map;
	}
}
