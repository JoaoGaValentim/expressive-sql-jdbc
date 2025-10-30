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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 * Classe responsável por gerir todas a conexão com a base de dados MySQL. Toda a conexão é feita
 * <p>
 * usando um arquivo <strong>.properties</strong>.
 * 
 * <p>
 * A classe é instanciada já iniciando tudo que precisa para funcionar:
 * 
 * <p>
 * - Conexão
 * - Load dos properties
 * 
 * @author joao
 * @since 0.2
 * @version 0.2
 * */


public class ConnectionFactory {
	private final Connection connection;
	private final List<String> params;
	private final List<Object> values;
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
			params = new ArrayList<>();
			values = new ArrayList<>();
		} catch (SQLException | IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * <p>
	 * Essa função carrega o .properties, desde que esteja dentro da pasta
	 * <p>
	 * <strong>src</strong> (Isso será fixado no futuro)
	 * @see #getPropertiesFile(String)
	 * **/
	private Properties getPropertiesFile(String propertyFile) throws IOException {
		Properties properties = new Properties();
		properties.load(ConnectionFactory.class.getResourceAsStream(propertyFile));
		return properties;
	}
	
	/**
	 * Essa função faz a injeção dos parâmetros no PreparedStatement.
	 *
	 *<p>
	 * - Faz casting de objeto para String, Double, Integer e Boolean
	 *<p>
	 * - Conta o total de <strong>index (i)</strong> que o Statement vai ter
	 * 
	 * @see #prepareValues(List, PreparedStatement)
	 * **/
	private void prepareValues(List<Object> objects, PreparedStatement statement) {
		try {
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
				i++;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Essa função faz a formatação de uma lista de parâmetros para:
	 * 
	 * <p>
	 * [name, price]
	 * 
	 * <p>
	 * (name, price)
	 * 
	 * @see #formatFields(List)
	 * **/
	public String formatFields(List<String> fields) {
		String formattedField = fields.toString().replace("[", "(");
		formattedField = formattedField.replace("]", ")");
		return formattedField;
	}
	
	/**
	 * Essa função faz a formatação de uma lista de Statements para:
	 * 
	 * <p>
	 * []
	 * 
	 * <p>
	 * (?, ?)
	 * 
	 * @see #agroupStatementField(int)
	 * **/
	public List<String> agroupStatementField(int totalFields) {
		List<String> statementFields = new ArrayList<>();
		for (int i = 0; i < totalFields; i++) {
			statementFields.add("?");
		}

		return statementFields;
	}

	public ConnectionFactory insertInto(String table, List<String> fields) {
		sql = "";
		sql = "INSERT INTO " + table;
		sql += formatFields(fields);
		return this;
	}

	public ConnectionFactory values(List<Object> objects) {
		if (objects == null) {
			throw new NullPointerException("Values is null. You need add not null value.");
		}

		List<String> statementFields = agroupStatementField(objects.size());
		sql += " VALUES " + formatFields(statementFields) + ";";

		try {
			statement = connection.prepareStatement(sql);
			prepareValues(values, statement);
			statement.execute();
			this.close();
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
	 
	public ConnectionFactory deleteFrom(String table) {
		sql = "";
		sql = "DELETE FROM " + table;
		return this;
	}

	public ConnectionFactory where() {
		sql += " WHERE ";
		return this;
	}

	public ConnectionFactory param(String param) {
		sql += param;
		params.add(param);
		return this;
	}

	public ConnectionFactory equalsValue(Object value) {
		sql += " = ? ";
		values.add(value);
		return this;
	}

	public ConnectionFactory biggerThan(Object value) {
		sql += " > ? ";
		values.add(value);
		return this;
	}

	public ConnectionFactory smallerThan(Object value) {
		sql += " < ? ";
		values.add(value);
		return this;
	}

	public ConnectionFactory biggerThanOrEquals(Object value) {
		sql += " >= ? ";
		values.add(value);
		return this;
	}

	public ConnectionFactory smallerThanOrEquals(Object value) {
		sql += " =< ? ";
		values.add(value);
		return this;
	}

	public ConnectionFactory differentFrom(Object value) {
		sql += " <> ? ";
		values.add(value);
		return this;
	}

	public ConnectionFactory likeLeft(Object value) {
		sql += " LIKE ? ";
		values.add("%" + value);
		return this;
	}

	public ConnectionFactory likeRight(Object value) {
		sql += " LIKE ? ";
		values.add(value + "%");
		return this;
	}

	public ConnectionFactory like(Object value) {
		sql += " LIKE ? ";
		values.add("%" + value + "%");
		System.out.println(sql);
		return this;
	}

	public ConnectionFactory and() {
		sql += "AND ";
		return this;
	}
	
	public boolean execute() {
		try {
			statement = connection.prepareStatement(sql);
			prepareValues(values, statement);
			values.clear();
			params.clear();
			statement.execute();
			this.close();
			return true;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Map<String, List<Object>> response() throws SQLException {
		if (statement == null) {
			sql += ";";
			statement = connection.prepareStatement(sql);
			if (params.size() == values.size() && values.size() > 0) {
				prepareValues(values, statement);
			}
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
		
		values.clear();
		params.clear();
		this.close();
		return map;
	}

	public void close() {
		try {
			if (rs != null && !rs.isClosed()) {
				rs.close();
			}
			
			if (statement != null && !statement.isClosed()) {
				statement.close();
			}
			
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
