package org.eclipse.jgit.integrate.remotehelper.internal;

import org.eclipse.jgit.integrate.remotehelper.RemoteHelperCommand;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.TextProgressMonitor;
import org.eclipse.jgit.transport.PushConnection;
import org.eclipse.jgit.transport.RemoteRefUpdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportPushCommand extends RemoteHelperCommand {
	private final RemoteHelperTransport provider;

	public TransportPushCommand(RemoteHelperTransport provider) {
		super(provider.getContext());

		this.provider = provider;
	}

	private List<PushDefinition> defs = new ArrayList<>();

	@Override
	public void handle(List<String> arguments) throws Exception {
		String first = arguments.get(0);
		if (first.startsWith("+")) {
			first = first.substring(1);
		}
		String[] parts = first.split(":", 2);
		defs.add(new PushDefinition(parts[0], parts[1]));
	}

	@Override
	public void complete() throws Exception {
		Map<String, RemoteRefUpdate> refUpdates = new HashMap<>();

		for (PushDefinition def : defs) {
			Ref local = getContext().getRepository().findRef(def.getSource());
			RemoteRefUpdate refUpdate = new RemoteRefUpdate(
				getContext().getRepository(),
				local,
				def.getDest(),
				false,
				def.getSource(),
				null
			);

			refUpdates.put(def.getDest(), refUpdate);
		}

		PushConnection connection = provider.getPushConnection();

		connection.push(
			new TextProgressMonitor(),
			refUpdates
		);

		for (String dest : refUpdates.keySet()) {
			RemoteRefUpdate refUpdate = refUpdates.get(dest);
			if (refUpdate.getStatus() == RemoteRefUpdate.Status.OK) {
				getContext().reply("ok " + dest);
			} else {
				getContext().reply("error " + dest + " " + refUpdate.getStatus().name());
			}
		}

		defs.clear();
		getContext().complete();
	}

	private static class PushDefinition {
		private final String source;
		private final String dest;

		PushDefinition(String src, String dst) {
			this.source = src;
			this.dest = dst;
		}

		public String getSource() {
			return source;
		}

		public String getDest() {
			return dest;
		}
	}
}
