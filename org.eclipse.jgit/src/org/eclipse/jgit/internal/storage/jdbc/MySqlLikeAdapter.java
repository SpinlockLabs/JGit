package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.util.StringUtils;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class MySqlLikeAdapter extends SqlDriverAdapter {
	protected String quote(String n) {
		return '`' + n + '`';
	}

	protected String columnNames(String... names) {
		ArrayList<String> realNames = new ArrayList<>(names.length);
		for (int i = 0; i < names.length; i++) {
			realNames.add(quote(names[i]));
		}
		return StringUtils.join(realNames, ",");
	}

	protected String getBlobType() {
		return "LONGBLOB";
	}

	protected String getHashType() {
		return "VARCHAR(512)";
	}

	protected String getRefType() {
		return "VARCHAR(512)";
	}

	@Override
	public boolean checkObjectsTableExists() throws SQLException {
		ResultSet results = getRepository().getConnection().prepareStatement(
			"SHOW TABLES"
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
			"SHOW TABLES"
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
	public PreparedStatement createInsertObjectBatch() throws SQLException {
		String query = "INSERT INTO " + quote(getObjectsTableName()) + " (" +
			columnNames(getObjectHashColumn(), getObjectTypeColumn(), getObjectContentColumn()) +
			") VALUES (?, ?, ?)";

		return getRepository().getConnection().prepareStatement(
			query
		);
	}

	@Override
	public void createInsertObjectBatch(PreparedStatement statement, String hash, int type, InputStream stream) throws SQLException {
		statement.setString(1, hash);
		statement.setInt(2, type);
		statement.setBlob(3, stream);
		statement.addBatch();
		statement.clearParameters();
	}

	@Override
	public PreparedStatement createReadObjectMeta(String hash) throws SQLException {
		PreparedStatement statement = getRepository().getConnection().prepareStatement(
			"SELECT " +
				quote(getObjectTypeColumn()) + ", LENGTH(" + quote(getObjectContentColumn()) +
				") AS " + quote(getObjectSizeColumn()) + " FROM " + quote(getObjectsTableName()) +
				" WHERE " + quote(getObjectHashColumn()) + " = ?"
		);

		statement.setString(1, hash);
		return statement;
	}

	@Override
	public PreparedStatement createReadObject(String hash) throws SQLException {
		String query =
			"SELECT " + quote(getObjectTypeColumn()) +
				", LENGTH(" + quote(getObjectContentColumn()) +
				") AS " + quote(getObjectSizeColumn()) + ", " + quote(getObjectContentColumn()) + " FROM " +
				quote(getObjectsTableName()) + " WHERE " + quote(getObjectHashColumn()) + " = ?";

		PreparedStatement statement = getRepository().getConnection().prepareStatement(query);

		statement.setString(1, hash);
		return statement;
	}

	@Override
	public PreparedStatement createFindAbbreviatedObject(String prefix) throws SQLException {
		PreparedStatement statement = getRepository().getConnection().prepareStatement(
			"SELECT " + quote(getObjectHashColumn()) +
				" FROM " + quote(getObjectsTableName()) + " WHERE " +
				quote(getObjectHashColumn()) + " LIKE ?"
		);

		statement.setString(1, prefix + "%");
		return statement;
	}

	@Override
	public PreparedStatement createObjectCount() throws SQLException {
		return getRepository().getConnection().prepareStatement(
			"SELECT COUNT(*) AS count FROM " + quote(getObjectsTableName())
		);
	}

	@Override
	public PreparedStatement createUpdateRef(String name, boolean symbolic, String target) throws SQLException {
		String query = "UPDATE " + quote(getRefsTableName()) + " SET "
			+ quote(getRefTargetColumn()) + " = ?, " + quote(getRefIsSymbolicColumn()) +
			" = ? WHERE " + quote(getRefNameColumn()) + " = ?";
		PreparedStatement statement = getRepository().getConnection().prepareStatement(query);
		statement.setString(1, target);
		statement.setBoolean(2, symbolic);
		statement.setString(3, name);
		return statement;
	}

	@Override
	public PreparedStatement createRef(String name, boolean symbolic, String target) throws SQLException {
		PreparedStatement statement = getRepository().getConnection().prepareStatement(
			"INSERT INTO " + quote(getRefsTableName()) + " (" + columnNames(
				getRefNameColumn(),
				getRefTargetColumn(),
				getRefIsSymbolicColumn()
			) + ") VALUES (?, ?, ?)"
		);
		statement.setString(1, name);
		statement.setString(2, target);
		statement.setBoolean(3, symbolic);
		return statement;
	}

	private boolean isRealMySql() throws SQLException {
		return getRepository().getConnection().getMetaData().getDriverName().contains("MySQL");
	}

	@Override
	public PreparedStatement createObjectsTable() throws SQLException {
		String query = "CREATE TABLE " + quote(getObjectsTableName()) + " (" +
			"" + quote(getObjectHashColumn()) + " " + getHashType();

		if (isRealMySql()) {
			query += " CHARACTER SET ascii COLLATE ascii_bin";
		}

		query += " NOT NULL PRIMARY KEY," +
			"" + quote(getObjectTypeColumn()) + " SMALLINT NOT NULL," +
			"" + quote(getObjectContentColumn()) + " " + getBlobType() + " NOT NULL" +
			")";

		return getRepository().getConnection().prepareStatement(query);
	}

	@Override
	public PreparedStatement createRefsTable() throws SQLException {
		String query = "CREATE TABLE " + quote(getRefsTableName()) + " (" +
			quote(getRefNameColumn()) + " " + getHashType();

		if (isRealMySql()) {
			query += " CHARACTER SET ascii COLLATE ascii_bin";
		}

		query += " NOT NULL PRIMARY KEY," +
			quote(getRefIsSymbolicColumn()) + " BOOLEAN NOT NULL," +
			quote(getRefTargetColumn()) + " " + getRefType() + " NOT NULL" +
			")";

		return getRepository().getConnection().prepareStatement(query);
	}

	@Override
	public PreparedStatement createDropObjectsTable() throws SQLException {
		return getRepository().getConnection().prepareStatement(
			"DROP TABLE " + quote(getObjectsTableName())
		);
	}

	@Override
	public PreparedStatement createDropRefsTable() throws SQLException {
		return getRepository().getConnection().prepareStatement(
			"DROP TABLE " + quote(getRefsTableName())
		);
	}

	@Override
	public PreparedStatement createReadRef(String name) throws SQLException {
		PreparedStatement statement = getRepository().getConnection().prepareStatement(
			"SELECT " + columnNames(
				getRefTargetColumn(),
				getRefIsSymbolicColumn(),
				getRefNameColumn()
			) + " FROM " + quote(getRefsTableName()) + " WHERE " + quote(getRefNameColumn()) + " = ?"
		);

		statement.setString(1, name);
		return statement;
	}

	@Override
	public PreparedStatement createReadRefs(String prefix) throws SQLException {
		PreparedStatement statement = getRepository().getConnection().prepareStatement(
			"SELECT " + columnNames(
				getRefNameColumn(),
				getRefTargetColumn(),
				getRefIsSymbolicColumn()
			) + " FROM " + quote(getRefsTableName()) + " WHERE " + quote(getRefNameColumn()) + " LIKE ?"
		);

		statement.setString(1, prefix + "%");
		return statement;
	}

	@Override
	public PreparedStatement createListRefs() throws SQLException {
		return getRepository().getConnection().prepareStatement(
			"SELECT " + columnNames(
				getRefNameColumn(),
				getRefTargetColumn(),
				getRefIsSymbolicColumn()
			) + " FROM " + quote(getRefsTableName())
		);
	}

	@Override
	public PreparedStatement createDeleteRef(String name) throws SQLException {
		PreparedStatement statement = getRepository().getConnection().prepareStatement(
			"DELETE FROM " + quote(getRefsTableName()) + " WHERE " + quote(getRefNameColumn()) + " = ?"
		);

		statement.setString(1, name);
		return statement;
	}

	@Override
	public PreparedStatement createLinkRef(String name, String target) throws SQLException {
		return createRef(name, true, target);
	}
}
