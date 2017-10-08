package org.eclipse.jgit.integrate.remotehelper.internal;

import org.eclipse.jgit.integrate.remotehelper.RemoteHelperCommand;
import org.eclipse.jgit.lib.Ref;

import java.util.List;
import java.util.Map;

public class TransportListCommand extends RemoteHelperCommand {
	private final RemoteHelperTransport provider;

	public TransportListCommand(RemoteHelperTransport provider) {
		super(provider.getContext());
		this.provider = provider;
	}

	@Override
	public void handle(List<String> arguments) throws Exception {
		Map<String, Ref> refs = provider.getFetchConnection().getRefsMap();

		for (String key : refs.keySet()) {
			Ref ref = refs.get(key);
			StringBuilder builder = new StringBuilder();

			if (ref.isSymbolic()) {
				builder.append('@').append(ref.getTarget().getName());
			} else if (ref.getObjectId() != null) {
				builder.append(ref.getObjectId().name());
			} else {
				builder.append('?');
			}

			builder.append(' ').append(key);
			getContext().reply(builder.toString());
		}

		getContext().complete();
	}
}
