package org.eclipse.jgit.storage.jdbc;

import org.eclipse.jgit.internal.storage.jdbc.SqlRepository;
import org.eclipse.jgit.lib.BaseRepositoryBuilder;
import org.eclipse.jgit.lib.Repository;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqlRepositoryBuilder extends BaseRepositoryBuilder<SqlRepositoryBuilder, Repository> {
	private Connection connection;

	private String url;
	private String username;
	private String password;

	@Override
	public Repository build() throws IOException {
		if (connection == null) {
			try {
				connection = DriverManager.getConnection(
					url,
					username,
					password
				);
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		return new SqlRepository(connection, this);
	}

	public SqlRepositoryBuilder setConnection(Connection connection) {
		this.connection = connection;
		return this;
	}

	public SqlRepositoryBuilder setDatabaseUrl(String url) {
		this.url = url;
		return this;
	}

	public SqlRepositoryBuilder setDatabaseUsername(String username) {
		this.username = username;
		return this;
	}

	public SqlRepositoryBuilder setDatabasePassword(String password) {
		this.password = password;
		return this;
	}
}
