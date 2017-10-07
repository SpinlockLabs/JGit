package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.util.StringUtils;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class MySqlAdapter extends SqlDriverAdapter {
	private static String quote(String n) {
		return '`' + n + '`';
	}

	private static String columnNames(String... names) {
		ArrayList<String> realNames = new ArrayList<>(names.length);
		for (int i = 0; i < names.length; i++) {
			realNames.add(quote(names[i]));
		}
		return StringUtils.join(realNames, ",");
	}

	@Override
	public boolean checkObjectsTableExists() throws SQLException {
		ResultSet results = getRepository().getConnection().prepareStatement(
			"SHOW TABLES LIKE '" + getObjectsTableName() + "'"
		).executeQuery();

		boolean exists = results.next();
		results.close();
		return exists;
	}

	@Override
	public boolean checkRefsTableExists() throws SQLException {
		ResultSet results = getRepository().getConnection().prepareStatement(
			"SHOW TABLES LIKE '" + getRefsTableName() + "'"
		).executeQuery();

		boolean exists = results.next();
		results.close();
		return exists;
	}

	@Override
	public PreparedStatement createInsertObject() throws SQLException {
		return getRepository().getConnection().prepareStatement(
			"INSERT INTO " + quote(getObjectsTableName()) + " (" +
				columnNames(getObjectHashColumn(), getObjectTypeColumn(), getObjectContentColumn()) +
				") VALUES (?, ?, ?)"
		);
	}

	@Override
	public void addInsertedObject(PreparedStatement statement, String hash, int type, InputStream stream) throws SQLException {
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
	public PreparedStatement createUpdateRef(String name, boolean symbolic, String target) throws SQLException {
		PreparedStatement statement = getRepository().getConnection().prepareStatement(
			"UPDATE " + quote(getRefsTableName()) + " SET "
				+ quote(getRefTargetColumn()) + " = ?, " + quote(getRefIsSymbolicColumn()) +
				" = ? WHERE " + quote(getRefNameColumn()) + " = ?"
		);
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

	@Override
	public PreparedStatement createObjectsTable() throws SQLException {
		String query = "CREATE TABLE " + quote(getObjectsTableName()) + " (" +
			"" + quote(getObjectHashColumn()) + " VARCHAR(512) CHAR SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY," +
			"" + quote(getObjectTypeColumn()) + " TINYINT(4) NOT NULL," +
			"" + quote(getObjectContentColumn()) + " LONGBLOB NOT NULL" +
			")";

		return getRepository().getConnection().prepareStatement(query);
	}

	@Override
	public PreparedStatement createRefsTable() throws SQLException {
		String query = "CREATE TABLE " + quote(getRefsTableName()) + " (" +
			quote(getRefNameColumn()) + " VARCHAR(512) CHAR SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY," +
			quote(getRefIsSymbolicColumn()) + " BOOLEAN NOT NULL," +
			quote(getRefTargetColumn()) + " VARCHAR(512) NOT NULL" +
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
