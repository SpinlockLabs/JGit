package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.attributes.AttributesNode;
import org.eclipse.jgit.attributes.AttributesNodeProvider;
import org.eclipse.jgit.attributes.AttributesRule;
import org.eclipse.jgit.lib.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

public class SqlRepository extends Repository {
	private static SqlDriverAdapter detectAdapter(Connection connection) {
		try {
			String name = connection.getMetaData().getDriverName();

			if (name.contains("SQLite")) {
				return new SqliteAdapter();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return new MySqlAdapter();
	}

	private final Connection connection;
	private final SqlDriverAdapter adapter;
	private final SqlObjectDatabase objectDatabase;
	private final SqlRefDatabase refDatabase;

	public SqlRepository(Connection connection) {
		this(connection, detectAdapter(connection));
	}

	public SqlRepository(Connection connection, BaseRepositoryBuilder builder) {
		this(connection, new MySqlAdapter(), builder);
	}

	public SqlRepository(Connection connection, SqlDriverAdapter adapter) {
		this(connection, adapter, new BaseRepositoryBuilder());
	}

	public SqlRepository(Connection connection, SqlDriverAdapter adapter, BaseRepositoryBuilder builder) {
		super(builder);

		this.connection = connection;
		this.adapter = adapter;
		this.objectDatabase = new SqlObjectDatabase(this);
		this.refDatabase = new SqlRefDatabase(this);

		adapter.setRepository(this);
	}

	@Override
	public void create(boolean bare) throws IOException {
		if (!bare) {
			throw new IOException("Non-bare Git repositories are not supported.");
		}

		objectDatabase.create();
		refDatabase.create();

		RefUpdate head = updateRef(Constants.HEAD);
		head.disableRefLog();
		head.link(Constants.R_HEADS + Constants.MASTER);
	}

	@Override
	public SqlObjectDatabase getObjectDatabase() {
		return objectDatabase;
	}

	@Override
	public SqlRefDatabase getRefDatabase() {
		return refDatabase;
	}

	@Override
	public StoredConfig getConfig() {
		return new StoredConfig() {
			@Override
			public void load() {
			}

			@Override
			public void save() {
			}
		};
	}

	@Override
	public AttributesNodeProvider createAttributesNodeProvider() {
		return new EmptyAttributesNodeProvider();
	}

	@Override
	public void scanForRepoChanges() throws IOException {
	}

	@Override
	public void notifyIndexChanged() {
	}

	@Override
	public ReflogReader getReflogReader(String refName) throws IOException {
		return null;
	}

	public Connection getConnection() {
		return connection;
	}

	public SqlDriverAdapter getAdapter() {
		return adapter;
	}

	public int getNumberOfObjects() throws IOException {
		return getObjectDatabase().getObjectCount();
	}

	private static class EmptyAttributesNodeProvider implements
		AttributesNodeProvider {
		private EmptyAttributesNode emptyAttributesNode = new EmptyAttributesNode();

		@Override
		public AttributesNode getInfoAttributesNode() throws IOException {
			return emptyAttributesNode;
		}

		@Override
		public AttributesNode getGlobalAttributesNode() throws IOException {
			return emptyAttributesNode;
		}

		private static class EmptyAttributesNode extends AttributesNode {

			public EmptyAttributesNode() {
				super(Collections.<AttributesRule> emptyList());
			}

			@Override
			public void parse(InputStream in) throws IOException {
				// Do nothing
			}
		}
	}
}
