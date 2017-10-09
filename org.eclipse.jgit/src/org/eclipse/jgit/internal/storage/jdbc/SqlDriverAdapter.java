package org.eclipse.jgit.internal.storage.jdbc;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class SqlDriverAdapter {
	private SqlRepository repository;

	public abstract boolean checkObjectsTableExists() throws SQLException;
	public abstract boolean checkRefsTableExists() throws SQLException;

	/**
	 * Create insert object prepared statement.
	 * @return prepared statement.
	 */
	public abstract PreparedStatement createInsertObject() throws SQLException;

	public abstract void addInsertedObject(
		PreparedStatement statement,
		String hash,
		int type,
		InputStream stream
	) throws SQLException;

	/**
	 * Creates a statement that reads object metadata
	 * (excluding the content) using the given parameters.
	 * @param hash object hash
	 * @return statement
	 */
	public abstract PreparedStatement createReadObjectMeta(
		String hash
	)  throws SQLException;

	public abstract PreparedStatement createReadObject(
		String hash
	) throws SQLException;

	public abstract PreparedStatement createFindAbbreviatedObject(
		String prefix
	) throws SQLException;

	public abstract PreparedStatement createObjectCount() throws SQLException;

	public abstract PreparedStatement createUpdateRef(String name, boolean symbolic, String target) throws SQLException;
	public abstract PreparedStatement createRef(String name, boolean symbolic, String target) throws SQLException;

	public abstract PreparedStatement createObjectsTable() throws SQLException;
	public abstract PreparedStatement createRefsTable() throws SQLException;

	public abstract PreparedStatement createDropObjectsTable() throws SQLException;
	public abstract PreparedStatement createDropRefsTable() throws SQLException;

	public abstract PreparedStatement createReadRef(String name) throws SQLException;
	public abstract PreparedStatement createReadRefs(String prefix) throws SQLException;
	public abstract PreparedStatement createListRefs() throws SQLException;
	public abstract PreparedStatement createDeleteRef(String name) throws SQLException;

	public abstract PreparedStatement createLinkRef(String name, String target) throws SQLException;

	public String getObjectHashColumn() {
		return "hash";
	}

	public String getObjectTypeColumn() {
		return "type";
	}

	public String getObjectSizeColumn() {
		return "size";
	}

	public String getObjectContentColumn() {
		return "content";
	}

	public String getRefNameColumn() {
		return "name";
	}

	public String getRefIsSymbolicColumn() {
		return "symbolic";
	}

	public String getRefTargetColumn() {
		return "target";
	}

	public String getObjectsTableName() {
		return "git.objects";
	}

	public String getRefsTableName() {
		return "git.refs";
	}

	public void setRepository(SqlRepository repository) {
		this.repository = repository;
	}

	public SqlRepository getRepository() {
		return repository;
	}
}
