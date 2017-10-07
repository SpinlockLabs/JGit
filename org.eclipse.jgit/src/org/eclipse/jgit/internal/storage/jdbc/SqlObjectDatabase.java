package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.errors.LargeObjectException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.transport.PackParser;
import org.eclipse.jgit.util.sha1.SHA1;

import java.io.*;
import java.sql.*;
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
			parent.getConnection().close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
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
					String oid = results.getString(0);
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
					throw new MissingObjectException(objectId.toObjectId(), typeHint);
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
					openStream().close();
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

				byte[] bytes = results.getBytes(parent.getAdapter().getObjectContentColumn());
				cacheLoaded = true;
				cachedSize = bytes.length;
				cachedBlobData = bytes;
				cachedType = results.getInt(parent.getAdapter().getObjectTypeColumn());
				statement.close();
				return new ObjectStream.SmallStream(cachedType, bytes);
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
	}

	public class SqlObjectInserter extends ObjectInserter {
		private PreparedStatement statement;

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

				if (statement == null) {
					statement = parent.getAdapter().createInsertObject();
				}

				parent.getAdapter().addInsertedObject(
					statement,
					id.name(),
					objectType,
					new ByteArrayInputStream(bytes)
				);
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
			if (statement == null) {
				return;
			}

			try {
				statement.executeBatch();
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void close() {
			if (statement == null) {
				return;
			}

			try {
				statement.executeBatch();
				statement.close();
			} catch (SQLException e) {
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

		public boolean isWriteToSecondary() {
			return writeToSecondary;
		}

		public void setWriteToSecondary(boolean writeToSecondary) {
			this.writeToSecondary = writeToSecondary;
		}
	}
}
