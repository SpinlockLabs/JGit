package org.eclipse.jgit.integrate.remotehelper.internal;

import org.eclipse.jgit.integrate.remotehelper.RemoteHelperCommand;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.TextProgressMonitor;
import org.eclipse.jgit.transport.FetchConnection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class TransportFetchCommand extends RemoteHelperCommand {
	private final RemoteHelperTransport provider;

	public TransportFetchCommand(RemoteHelperTransport provider) {
		super(provider.getContext());
		this.provider = provider;
	}

	private ArrayList<FetchDefinition> defs = new ArrayList<>();

	@Override
	public void handle(List<String> arguments) throws Exception {
		defs.add(new FetchDefinition(arguments.get(0), arguments.get(1)));
	}

	@Override
	public void complete() throws Exception {
		List<Ref> want = new ArrayList<>();
		HashSet<ObjectId> have = new HashSet<>();

		for (FetchDefinition def : defs) {
			want.add(provider.getFetchConnection().getRef(def.getName()));
		}

		FetchConnection connection = provider.getFetchConnection();
		connection.fetch(
			new TextProgressMonitor(),
			want,
			have
		);

		connection.close();

		defs.clear();
		getContext().complete();
	}

	private static class FetchDefinition {
		private final String sha;
		private final String name;

		FetchDefinition(String sha, String name) {
			this.sha = sha;
			this.name = name;
		}

		public String getSha() {
			return sha;
		}

		public String getName() {
			return name;
		}
	}
}
