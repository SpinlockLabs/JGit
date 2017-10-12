package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefDatabase;
import org.eclipse.jgit.lib.RefUpdate;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SqlRefUpdate extends RefUpdate {
	private final SqlRefDatabase database;

	public SqlRefUpdate(final Ref ref, final SqlRefDatabase database) {
		super(ref);
		this.database = database;
	}

	@Override
	protected RefDatabase getRefDatabase() {
		return database;
	}

	@Override
	protected SqlRepository getRepository() {
		return database.getRepository();
	}

	@Override
	protected boolean tryLock(boolean deref) throws IOException {
		return true;
	}

	@Override
	protected void unlock() {
	}

	@Override
	protected Result doUpdate(Result status) throws IOException {
		try {
			PreparedStatement statement = getRepository().getAdapter().createReadRef(getName());
			ResultSet results = statement.executeQuery();
			boolean exists = results.next();
			results.close();

			if (exists) {
				statement = getRepository().getAdapter().createUpdateRef(
					getName(),
					getRef().isSymbolic(),
					getNewObjectId().name()
				);

				if (statement.executeUpdate() == 0) {
					return Result.NO_CHANGE;
				} else {
					return status;
				}
			} else {
				statement = getRepository().getAdapter().createRef(
					getName(),
					getRef().isSymbolic(),
					getNewObjectId().name()
				);

				if (statement.executeUpdate() == 0) {
					return Result.REJECTED;
				} else {
					return status;
				}
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	protected Result doDelete(Result status) throws IOException {
		try {
			PreparedStatement statement = getRepository().getAdapter().createDeleteRef(
				getName()
			);

			statement.setString(1, getRef().getName());
			if (statement.executeUpdate() == 0) {
				return Result.REJECTED;
			}
			return status;
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	protected Result doLink(String target) throws IOException {
		try {
			PreparedStatement statement = getRepository().getAdapter().createReadRef(getName());
			ResultSet results = statement.executeQuery();
			boolean exists = results.next();
			results.close();

			if (exists) {
				statement = getRepository().getAdapter().createUpdateRef(
					getName(),
					true,
					target
				);

				if (statement.executeUpdate() == 0) {
					return Result.REJECTED;
				}
				return Result.NEW;
			} else {
				statement = getRepository().getAdapter().createLinkRef(
					getName(),
					target
				);

				if (statement.executeUpdate() == 0) {
					return Result.REJECTED;
				}
				return Result.NEW;
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
}
