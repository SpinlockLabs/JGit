package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.attributes.AttributesNodeProvider;
import org.eclipse.jgit.lib.*;

import java.io.IOException;
import java.sql.Connection;

public class SqlRepository extends Repository {
	private final Connection connection;
	private final SqlDriverAdapter adapter;
	private final SqlObjectDatabase objectDatabase;
	private final SqlRefDatabase refDatabase;

	public SqlRepository(Connection connection) {
		this(connection, new MySqlAdapter());
	}

	public SqlRepository(Connection connection, SqlDriverAdapter adapter) {
		super(new BaseRepositoryBuilder());

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
		return null;
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
}
