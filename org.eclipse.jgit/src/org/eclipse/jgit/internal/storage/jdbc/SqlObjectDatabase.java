package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.errors.LargeObjectException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.transport.PackParser;
import org.eclipse.jgit.util.IO;
import org.eclipse.jgit.util.sha1.SHA1;

import java.io.*;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class SqlObjectDatabase extends ObjectDatabase {
	private final SqlRepository parent;

	public SqlObjectDatabase(SqlRepository parent) {
		this.parent = parent;
	}

	@Override
	public void create() throws IOException {
		try {
			if (exists()) {
				PreparedStatement dropTable = parent.getAdapter().createDropObjectsTable();
				dropTable.execute();
			}

			PreparedStatement create = parent.getAdapter().createObjectsTable();
			create.execute();
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean exists() {
		try {
			return parent.getAdapter().checkObjectsTableExists();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ObjectInserter newInserter() {
		return new SqlObjectInserter();
	}

	@Override
	public ObjectReader newReader() {
		return new SqlObjectReader();
	}

	@Override
	public void close() {
		try {
			if (!parent.getConnection().getAutoCommit()) {
				parent.getConnection().commit();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public long getObjectCount() throws IOException {
		try {
			PreparedStatement statement = parent.getAdapter().createObjectCount();
			ResultSet results = statement.executeQuery();

			if (!results.next()) {
				return -1;
			}

			return results.getLong(1);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	public class SqlObjectReader extends ObjectReader {
		@Override
		public ObjectReader newReader() {
			return new SqlObjectReader();
		}

		@Override
		public Collection<ObjectId> resolve(AbbreviatedObjectId id) throws IOException {
			try {
				PreparedStatement statement = parent.getAdapter().createFindAbbreviatedObject(
					id.name()
				);

				ArrayList<ObjectId> ids = new ArrayList<>();
				ResultSet results = statement.executeQuery();

				while (results.next()) {
					String oid = results.getString(1);
					ids.add(ObjectId.fromString(oid));
				}
				return ids;
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		@Override
		public ObjectLoader open(AnyObjectId objectId, int typeHint) throws IOException {
			SqlObjectLoader loader = new SqlObjectLoader(objectId, typeHint);
			loader.loadCache();
			return loader;
		}

		@Override
		public Set<ObjectId> getShallowCommits() throws IOException {
			return new HashSet<>();
		}

		@Override
		public void close() {
		}
	}

	public class SqlObjectLoader extends ObjectLoader {
		private final AnyObjectId objectId;
		private final int typeHint;

		private int cachedType;
		private long cachedSize;
		private byte[] cachedBlobData;

		private boolean cacheLoaded = false;

		public SqlObjectLoader(AnyObjectId objectId, int typeHint) {
			this.objectId = objectId;
			this.typeHint = typeHint;

			cachedBlobData = null;
		}

		private void loadCache() throws IOException {
			try {
				PreparedStatement statement = parent.getAdapter().createReadObjectMeta(
					objectId.name()
				);
				ResultSet results = statement.executeQuery();
				if (!results.next()) {
					if (typeHint == Constants.OBJ_BAD) {
						throw new MissingObjectException(objectId.toObjectId(), "unknown");
					} else {
						throw new MissingObjectException(objectId.toObjectId(), typeHint);
					}
				}

				cachedSize = results.getLong(parent.getAdapter().getObjectSizeColumn());
				cachedType = results.getInt(parent.getAdapter().getObjectTypeColumn());
				cacheLoaded = true;
				statement.close();
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		@Override
		public int getType() {
			if (!cacheLoaded) {
				try {
					loadCache();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			return cachedType;
		}

		@Override
		public long getSize() {
			if (!cacheLoaded) {
				try {
					loadCache();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			return cachedSize;
		}

		@Override
		public byte[] getCachedBytes() throws LargeObjectException {
			if (cachedBlobData == null) {
				try {
					ByteBuffer buffer = IO.readWholeStream(openStream(), (int) getSize());
					return cachedBlobData = buffer.array();
				} catch (IOException e) {
					throw new LargeObjectException();
				}
			}
			return cachedBlobData;
		}

		@Override
		public ObjectStream openStream() throws IOException {
			if (cachedBlobData != null) {
				return new ObjectStream.SmallStream(getType(), getCachedBytes());
			}

			try {
				PreparedStatement statement = parent.getAdapter().createReadObject(
					objectId.name()
				);
				ResultSet results = statement.executeQuery();
				if (!results.next()) {
					throw new MissingObjectException(objectId.toObjectId(), typeHint);
				}


				if (parent.getAdapter().canUseBlob()) {
					Blob blob = results.getBlob(parent.getAdapter().getObjectContentColumn());
					cacheLoaded = true;
					cachedSize = blob.length();
					cachedType = results.getInt(parent.getAdapter().getObjectTypeColumn());
					return new BlobObjectStream(
						blob,
						statement,
						cachedType
					);
				} else {
					byte[] bytes = results.getBytes(parent.getAdapter().getObjectContentColumn());
					cacheLoaded = true;
					cachedSize = bytes.length;
					cachedType = results.getInt(parent.getAdapter().getObjectTypeColumn());
					cachedBlobData = bytes;
					return new ObjectStream.SmallStream(cachedType, cachedBlobData);
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
	}

	public class SqlObjectInserter extends ObjectInserter {
		private PreparedStatement cachedStatement;

		void writeHeader(OutputStream out, final int type, long len)
				throws IOException {
			out.write(Constants.encodedTypeString(type));
			out.write((byte) ' ');
			out.write(Constants.encodeASCII(len));
			out.write((byte) 0);
		}

		@Override
		public ObjectId insert(int objectType, long length, InputStream in) throws IOException {
			try {
				ByteArrayOutputStream out = new ByteArrayOutputStream();

				SHA1 sha = digest();
				SHA1OutputStream shaOut = new SHA1OutputStream(out, sha);
				shaOut.setWriteToSecondary(false);

				writeHeader(shaOut, objectType, length);
				{
					shaOut.setWriteToSecondary(true);
					int nRead;
					byte[] data = buffer();

					while ((nRead = in.read(data, 0, data.length)) != -1) {
						shaOut.write(data, 0, nRead);
					}
					shaOut.flush();
				}

				byte[] bytes = out.toByteArray();
				ObjectId id = sha.toObjectId();

				if (parent.getAdapter().doesSupportBatchInsertObject()) {
					if (cachedStatement == null) {
						cachedStatement = parent.getAdapter().createInsertObjectBatch();
					}

					parent.getAdapter().createInsertObjectBatch(
						cachedStatement,
						id.name(),
						objectType,
						new ByteArrayInputStream(bytes)
					);
				} else {
					PreparedStatement statement = parent.getAdapter().createInsertObject(
						id.name(),
						objectType,
						new ByteArrayInputStream(bytes)
					);

					if (statement.executeUpdate() != 1) {
						throw new SQLException("Failed to insert object " + id.name());
					}
					statement.close();
				}
				return id;
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		@Override
		public PackParser newPackParser(InputStream in) throws IOException {
			return null;
		}

		@Override
		public ObjectReader newReader() {
			return new SqlObjectReader();
		}

		@Override
		public void flush() throws IOException {
			try {
				if (!parent.getConnection().getAutoCommit()) {
					parent.getConnection().commit();
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}

			if (cachedStatement == null) {
				return;
			}

			try {
				cachedStatement.executeBatch();
			} catch (SQLException e) {
				throw new IOException(e);
			}

			cachedStatement = null;
		}

		@Override
		public void close() {
			try {
				flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static class SHA1OutputStream extends FilterOutputStream {
		private final SHA1 md;

		private boolean writeToSecondary = true;

		SHA1OutputStream(OutputStream out, SHA1 md) {
			super(out);
			this.md = md;
		}

		@Override
		public void write(int b) throws IOException {
			md.update((byte) b);

			if (writeToSecondary) {
				out.write(b);
			}
		}

		@Override
		public void write(byte[] in, int p, int n) throws IOException {
			md.update(in, p, n);

			if (writeToSecondary) {
				out.write(in, p, n);
			}
		}

		@Override
		public void flush() throws IOException {
			out.flush();
		}

		public void setWriteToSecondary(boolean writeToSecondary) {
			this.writeToSecondary = writeToSecondary;
		}
	}
}
