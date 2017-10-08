package org.eclipse.jgit.transport;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.errors.NotSupportedException;
import org.eclipse.jgit.errors.TransportException;
import org.eclipse.jgit.internal.storage.file.PackLock;
import org.eclipse.jgit.internal.storage.jdbc.SqlRepository;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.treewalk.TreeWalk;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class TransportJdbc extends Transport {
	static final TransportProtocol PROTO_JDBC = new TransportProtocol() {
		@Override
		public String getName() {
			return "JDBC";
		}

		@Override
		public Set<String> getSchemes() {
			Set<String> schemes = new HashSet<>();
			schemes.add("jdbc:mysql");
			schemes.add("mysql");
			schemes.add("jdbc:h2");
			return schemes;
		}

		@Override
		public Set<URIishField> getRequiredFields() {
			Set<URIishField> fields = new HashSet<>();
			fields.add(URIishField.HOST);
			fields.add(URIishField.PATH);
			return fields;
		}

		@Override
		public Transport open(URIish uri, Repository local, String remoteName) throws NotSupportedException, TransportException {
			String username = uri.getUser();

			if ("mysql".equals(uri.getScheme())) {
				uri = uri.setScheme("jdbc:mysql");
			}

			if (username == null) {
				username = local.getConfig().getString(
					"remote",
					remoteName,
					"db-username"
				);
			}

			String password = uri.getPass();

			if (password == null) {
				password = local.getConfig().getString(
					"remote",
					remoteName,
					"db-password"
				);
			}

			String query = local.getConfig().getString(
				"remote",
				remoteName,
				"db-options"
			);

			String url = uri.toASCIIString();

			if (query != null) {
				url += "?" + query;
			}

			try {
				Connection connection = DriverManager.getConnection(
					url,
					username,
					password
				);

				return new TransportJdbc(local, uri, connection);
			} catch (SQLException e) {
				throw new TransportException("Failed to get a JDBC connection.", e);
			}
		}
	};

	private final SqlRepository sql;

	protected TransportJdbc(Repository local, URIish uri, Connection connection) {
		super(local, uri);

		this.sql = new SqlRepository(connection);
	}

	public SqlRepository getSqlRepository() {
		return sql;
	}

	@Override
	public FetchConnection openFetch() throws NotSupportedException, TransportException {
		return new SqlFetchConnection();
	}

	@Override
	public PushConnection openPush() throws NotSupportedException, TransportException {
		return new SqlPushConnection();
	}

	@Override
	public void close() {
		sql.close();
	}

	abstract class SqlConnection implements org.eclipse.jgit.transport.Connection {
		@Override
		public Map<String, Ref> getRefsMap() {
			return getSqlRepository().getAllRefs();
		}

		@Override
		public Collection<Ref> getRefs() {
			return getSqlRepository().getAllRefs().values();
		}

		@Override
		public Ref getRef(String name) {
			try {
				return getSqlRepository().findRef(name);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public String getMessages() {
			return null;
		}

		@Override
		public String getPeerUserAgent() {
			return null;
		}

		@Override
		public void close() {
		}
	}

	class SqlPushConnection extends SqlConnection implements PushConnection {
		@Override
		public void push(ProgressMonitor monitor, Map<String, RemoteRefUpdate> refUpdates) throws TransportException {
			push(monitor, refUpdates, null);
		}

		@Override
		public void push(ProgressMonitor monitor, Map<String, RemoteRefUpdate> refUpdates, OutputStream out) throws TransportException {
			if (local == null) {
				throw new TransportException("JDBC transport does not work without a local repository.");
			}
			
			try {
				ObjectReader reader = local.getObjectDatabase().newReader();

				Set<ObjectId> objects = new HashSet<>();

				for (String key : refUpdates.keySet()) {
					RemoteRefUpdate update = refUpdates.get(key);
					ObjectId id = update.getNewObjectId();
					ObjectId oldId = update.getExpectedOldObjectId();

					if (AnyObjectId.equals(ObjectId.zeroId(), id)) {
						RefUpdate refUpdate = getSqlRepository().updateRef(key);
						refUpdate.delete();
						continue;
					}

					LogCommand log = Git.wrap(local).log();
					log.add(id);

					for (RevCommit commit : log.call()) {
						if (monitor.isCancelled()) {
							return;
						}

						RevTree tree = commit.getTree();
						TreeWalk walk = new TreeWalk(local);
						walk.addTree(tree);
						walk.setRecursive(true);
						walk.setPostOrderTraversal(true);

						while (walk.next()) {
							ObjectId treeId = walk.getObjectId(0);

							if (!getSqlRepository().hasObject(treeId)) {
								objects.add(treeId.copy());
							}
						}

						if (!getSqlRepository().hasObject(tree)) {
							objects.add(tree.copy());
						}

						if (!getSqlRepository().hasObject(commit)) {
							objects.add(commit.copy());
						}

						if (oldId != null && oldId.name().equals(commit.name())) {
							break;
						}
					}
				}

				ObjectInserter inserter = getSqlRepository().newObjectInserter();
				for (ObjectId send : objects) {
					ObjectLoader loader = reader.open(send);

					inserter.insert(loader.getType(), loader.getSize(), loader.openStream());
				}

				inserter.flush();
				inserter.close();
				reader.close();

				for (String refName : refUpdates.keySet()) {
					RemoteRefUpdate update = refUpdates.get(refName);
					RefUpdate refUpdate = getSqlRepository().updateRef(refName);

					refUpdate.setNewObjectId(update.getNewObjectId());
					refUpdate.setExpectedOldObjectId(update.getExpectedOldObjectId());
					refUpdate.setForceUpdate(update.isForceUpdate());

					RefUpdate.Result result = refUpdate.update();

					if (result == RefUpdate.Result.FAST_FORWARD ||
						result == RefUpdate.Result.NEW) {
						update.setStatus(RemoteRefUpdate.Status.OK);
					}

					if (result == RefUpdate.Result.NO_CHANGE) {
						update.setStatus(RemoteRefUpdate.Status.UP_TO_DATE);
					}
				}
			} catch (Exception e) {
				throw new TransportException("Failed to push.", e);
			}
		}
	}

	class SqlFetchConnection extends SqlConnection implements FetchConnection {
		@Override
		public void fetch(ProgressMonitor monitor, Collection<Ref> want, Set<ObjectId> have) throws TransportException {
			fetch(monitor, want, have, null);
		}

		@Override
		public void fetch(ProgressMonitor monitor, Collection<Ref> want, Set<ObjectId> have, OutputStream out) throws TransportException {
			if (local == null) {
				throw new TransportException("JDBC transport does not work without a local repository.");
			}

			ObjectReader reader = getSqlRepository().getObjectDatabase().newReader();
			ObjectInserter inserter = local.newObjectInserter();
			Set<ObjectId> objects = new HashSet<>();

			try {
				monitor.beginTask("Resolve Needed Objects", 1);

				{
					LogCommand log = Git.wrap(getSqlRepository()).log();

					for (Ref ref : want) {
						log.add(ref.getObjectId().copy());
					}

					for (RevCommit commit : log.call()) {
						if (monitor.isCancelled()) {
							return;
						}

						RevTree tree = commit.getTree();
						TreeWalk walk = new TreeWalk(reader);
						walk.addTree(tree);
						walk.setRecursive(true);
						walk.setPostOrderTraversal(true);

						while (walk.next()) {
							objects.add(walk.getObjectId(0).copy());
						}

						objects.add(tree.copy());
						objects.add(commit.copy());
					}
				}

				monitor.endTask();

				monitor.beginTask("Fetch Objects", 1);
				for (ObjectId id : objects) {
					if (monitor.isCancelled()) {
						return;
					}

					if (local.hasObject(id)) {
						continue;
					}

					ObjectLoader loader = reader.open(id);
					inserter.insert(loader.getType(), loader.getBytes());
				}

				inserter.flush();
				inserter.close();
				reader.close();

				monitor.endTask();
			} catch (Exception e) {
				throw new TransportException("Failed to fetch.", e);
			}
		}

		@Override
		public boolean didFetchIncludeTags() {
			return false;
		}

		@Override
		public boolean didFetchTestConnectivity() {
			return true;
		}

		@Override
		public void setPackLockMessage(String message) {
		}

		@Override
		public Collection<PackLock> getPackLocks() {
			return Collections.emptyList();
		}
	}
}
