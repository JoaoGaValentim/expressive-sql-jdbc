package br.com.jsql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import br.com.jsql.core.ConnectionFactory;

public class ConnectionTest {
	public static void main(String[] args) throws SQLException {
		ConnectionFactory connectionFactory = new ConnectionFactory("/jdbc-properties.properties");

		System.out.println(
			connectionFactory.deleteFrom("categories")
							 .where()
							 .param("id")
							 .equalsValue(8)
							 .execute()
		);
		
		connectionFactory.close();
	}

}
