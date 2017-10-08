package org.eclipse.jgit.integrate.remotehelper;

import java.util.Collection;

public abstract class RemoteHelperProvider {
	private final RemoteHelperContext context;

	public RemoteHelperProvider(RemoteHelperContext context) {
		this.context = context;

		context.setProvider(this);
	}

	public abstract RemoteHelperCommand getCommand(String name);
	public abstract Collection<RemoteHelperCapability> getCapabilities();

	public RemoteHelperContext getContext() {
		return context;
	}

	public abstract void close();
}
