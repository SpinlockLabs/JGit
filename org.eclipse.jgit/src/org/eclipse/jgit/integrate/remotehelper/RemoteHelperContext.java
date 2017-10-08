package org.eclipse.jgit.integrate.remotehelper;

import org.eclipse.jgit.lib.ProgressMonitor;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.URIish;

public interface RemoteHelperContext {
	void reply(String data);
	void complete();

	Repository getRepository();

	String getRemoteName();
	URIish getUrl();

	RemoteHelperProvider getProvider();
	void setProvider(RemoteHelperProvider provider);

	ProgressMonitor getProgressMonitor();

	void close();
}
