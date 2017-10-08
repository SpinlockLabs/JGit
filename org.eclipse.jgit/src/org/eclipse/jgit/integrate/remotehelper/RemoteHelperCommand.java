package org.eclipse.jgit.integrate.remotehelper;

import java.util.List;

public abstract class RemoteHelperCommand {
	private final RemoteHelperContext context;

	public RemoteHelperCommand(RemoteHelperContext context) {
		this.context = context;
	}

	public boolean isBatched() {
		return false;
	}

	public abstract void handle(
		List<String> arguments
	) throws Exception;

	public void complete() throws Exception {}

	public RemoteHelperContext getContext() {
		return context;
	}
}
