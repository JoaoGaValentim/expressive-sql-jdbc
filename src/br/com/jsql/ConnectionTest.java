package br.com.jsql;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import br.com.jsql.core.ConnectionFactory;

public class ConnectionTest {
	public static void main(String[] args) throws SQLException {
		ConnectionFactory connectionFactory = new ConnectionFactory("/jdbc-properties.properties");

		Map<String, List<Object>> data = connectionFactory.selectFrom("categories")
				.whereGet("id", "=", 3);

		for (Entry<String, List<Object>> object : data.entrySet()) {
			System.out.println(object.getKey() + " : " + object.getValue().getFirst());
		}
	}
}
