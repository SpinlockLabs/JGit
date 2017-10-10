package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.util.IO;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SqliteAdapter extends MySqlLikeAdapter {
	@Override
	public boolean checkObjectsTableExists() throws SQLException {
		ResultSet results = getRepository().getConnection().prepareStatement(
			"SELECT name FROM sqlite_master WHERE type = 'table'"
		).executeQuery();

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
		ResultSet results = getRepository().getConnection().prepareStatement(
			"SELECT name FROM sqlite_master WHERE type = 'table'"
		).executeQuery();

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
	public boolean doesSupportBatchInsertObject() {
		return false;
	}

	@Override
	public PreparedStatement createInsertObject(
		String hash,
		int type,
		InputStream stream
	) throws SQLException {
		String query = "INSERT INTO " + quote(getObjectsTableName()) + " (" +
			columnNames(getObjectHashColumn(), getObjectTypeColumn(), getObjectContentColumn()) +
			") VALUES (?, ?, ?)";

		PreparedStatement statement = getRepository().getConnection().prepareStatement(
			query
		);

		try {
			ByteBuffer buffer = IO.readWholeStream(stream, 0);
			statement.setString(1, hash);
			statement.setInt(2, type);
			statement.setBytes(3, buffer.array());
		} catch (IOException e) {
			throw new SQLException(e);
		}

		return statement;
	}
}
