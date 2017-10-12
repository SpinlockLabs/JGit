package org.eclipse.jgit.internal.storage.jdbc.adapter;

import org.eclipse.jgit.util.IO;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresAdapter extends MySqlLikeAdapter {
	@Override
	protected String quote(String n) {
		return '"' + n + '"';
	}

	@Override
	protected String getHashType() {
		return "TEXT";
	}

	@Override
	protected String getRefType() {
		return "TEXT";
	}

	@Override
	public void applyToConnection(Connection connection) throws SQLException {
		connection.setAutoCommit(false);
	}

	@Override
	protected String getBlobType() {
		return "BYTEA";
	}

	@Override
	public boolean checkObjectsTableExists() throws SQLException {
		PreparedStatement statement = getRepository().getConnection().prepareStatement(
			"SELECT tablename FROM pg_catalog.pg_tables WHERE tablename=?"
		);

		statement.setString(1, getObjectsTableName());

		ResultSet results = statement.executeQuery();

		while (results.next()) {
			if (getObjectsTableName().equalsIgnoreCase(results.getString(1))) {
				results.close();
				return true;
			}
		}

		results.close();
		return false;
	}

	@Override
	public boolean checkRefsTableExists() throws SQLException {
		PreparedStatement statement = getRepository().getConnection().prepareStatement(
			"SELECT tablename FROM pg_catalog.pg_tables WHERE tablename=?"
		);

		statement.setString(1, getRefsTableName());

		ResultSet results = statement.executeQuery();

		while (results.next()) {
			if (getRefsTableName().equalsIgnoreCase(results.getString(1))) {
				results.close();
				return true;
			}
		}

		results.close();
		return false;
	}

	@Override
	public void createInsertObjectBatch(
		PreparedStatement statement,
		String hash,
		int type,
		InputStream stream
	) throws SQLException {
		statement.setString(1, hash);
		statement.setInt(2, type);
		try {
			statement.setBytes(3, IO.readWholeStream(stream, 0).array());
		} catch (IOException e) {
			throw new SQLException(e);
		}
		statement.addBatch();
		statement.clearParameters();
	}

	@Override
	public boolean canUseBlob() {
		return false;
	}
}
