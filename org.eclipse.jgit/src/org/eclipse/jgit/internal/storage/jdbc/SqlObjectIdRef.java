package org.eclipse.jgit.internal.storage.jdbc;

import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef;

public class SqlObjectIdRef extends ObjectIdRef {
	protected SqlObjectIdRef(Storage st, String name, ObjectId id) {
		super(st, name, id);
	}

	@Override
	public ObjectId getPeeledObjectId() {
		return null;
	}

	@Override
	public boolean isPeeled() {
		return false;
	}
}
