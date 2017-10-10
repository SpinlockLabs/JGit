package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.lib.ObjectStream;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Statement;

public class BlobObjectStream extends ObjectStream {
	private final Blob blob;
	private final Statement owner;

	private final int type;

	private InputStream dataStream;

	public BlobObjectStream(Blob blob, Statement owner, int type) {
		this.blob = blob;
		this.owner = owner;
		this.type = type;
	}

	@Override
	public int getType() {
		return type;
	}

	@Override
	public long getSize() {
		try {
			return blob.length();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int read() throws IOException {
		if (dataStream == null) {
			try {
				dataStream = blob.getBinaryStream();
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		return dataStream.read();
	}

	@Override
	public void close() throws IOException {
		if (owner != null) {
			try {
				owner.close();
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		dataStream.close();
	}
}
