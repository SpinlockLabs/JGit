package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.lib.*;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlRefDatabase extends RefDatabase {
	private final SqlRepository parent;

	public SqlRefDatabase(SqlRepository parent) {
		this.parent = parent;
	}

	@Override
	public void create() throws IOException {
		try {
			if (parent.getAdapter().checkRefsTableExists()) {
				PreparedStatement drop = parent.getAdapter().createDropRefsTable();
				drop.execute();
			}

			PreparedStatement create = parent.getAdapter().createRefsTable();
			create.execute();
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean isNameConflicting(String name) throws IOException {
		try {
			PreparedStatement statement = parent.getAdapter().createReadRef(name);
			ResultSet results = statement.executeQuery();
			boolean conflicts = false;
			if (results.next()) {
				conflicts = true;
			}
			statement.close();
			return conflicts;
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public RefUpdate newUpdate(String name, boolean detach) throws IOException {
		Ref ref = getRef(name);

		if (ref == null) {
			ref = new ObjectIdRef.Unpeeled(Ref.Storage.NEW, name, null);
		}

		return new SqlRefUpdate(ref);
	}

	@Override
	public RefRename newRename(String fromName, String toName) throws IOException {
		return null;
	}

	@Override
	public Ref getRef(String name) throws IOException {
		try {
			PreparedStatement statement = parent.getAdapter().createReadRef(name);
			ResultSet results = statement.executeQuery();

			if (!results.next()) {
				statement.close();
				return null;
			}

			boolean symbolic = results.getBoolean(parent.getAdapter().getRefIsSymbolicColumn());
			String target = results.getString(parent.getAdapter().getRefTargetColumn());
			statement.close();

			if (symbolic) {
				Ref targetRef = getRef(target);
				if (targetRef == null) {
					return new SymbolicRef(name, new ObjectIdRef.Unpeeled(
						Ref.Storage.LOOSE,
						target,
						null
					));
				}
				return new SymbolicRef(name, targetRef);
			} else {
				return new SqlObjectIdRef(Ref.Storage.LOOSE, name, ObjectId.fromString(target));
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public Map<String, Ref> getRefs(String prefix) throws IOException {
		try {
			PreparedStatement statement = parent.getAdapter().createReadRefs(prefix);
			ResultSet results = statement.executeQuery();

			if (!results.next()) {
				statement.close();
				return null;
			}

			HashMap<String, Ref> refs = new HashMap<>();
			do {
				String name = results.getString(parent.getAdapter().getRefNameColumn());
				refs.put(name, getRef(name));
			} while(results.next());
			statement.close();
			return refs;
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public List<Ref> getAdditionalRefs() throws IOException {
		return null;
	}

	@Override
	public Ref peel(Ref ref) throws IOException {
		return null;
	}

	@Override
	public void close() {
		try {
			parent.getConnection().close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public class SqlRefUpdate extends RefUpdate {
		public SqlRefUpdate(final Ref ref) {
			super(ref);
		}

		@Override
		protected RefDatabase getRefDatabase() {
			return SqlRefDatabase.this;
		}

		@Override
		protected SqlRepository getRepository() {
			return parent;
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
						getNewObjectId().name(),
						getRef().isSymbolic(),
						getName()
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
				PreparedStatement statement = getRepository().getAdapter().createLinkRef(
					getName(),
					target
				);

				if (!statement.execute()) {
					return Result.REJECTED;
				}
				return Result.NEW;
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
	}
}
