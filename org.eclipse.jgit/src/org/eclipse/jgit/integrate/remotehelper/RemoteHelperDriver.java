package org.eclipse.jgit.integrate.remotehelper;

import org.eclipse.jgit.lib.ProgressMonitor;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TextProgressMonitor;
import org.eclipse.jgit.transport.URIish;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RemoteHelperDriver implements RemoteHelperContext {
	private final Repository repository;
	private final URIish uri;
	private final PrintWriter out;
	private final String remoteName;
	private final ProgressMonitor monitor;

	private RemoteHelperProvider provider;

	public RemoteHelperDriver(Repository repository, URIish uri, String remoteName, PrintWriter out, ProgressMonitor monitor) {
		this.repository = repository;
		this.uri = uri;
		this.remoteName = remoteName;
		this.out = out;
		this.monitor = monitor;
	}

	@Override
	public void reply(String data) {
		out.println(data);
		out.flush();
	}

	@Override
	public void complete() {
		out.println();
		out.flush();
	}

	@Override
	public Repository getRepository() {
		return repository;
	}

	@Override
	public String getRemoteName() {
		return remoteName;
	}

	@Override
	public URIish getUrl() {
		return uri;
	}

	@Override
	public RemoteHelperProvider getProvider() {
		return provider;
	}

	@Override
	public void setProvider(RemoteHelperProvider provider) {
		this.provider = provider;
	}

	@Override
	public ProgressMonitor getProgressMonitor() {
		return monitor;
	}

	@Override
	public void close() {
		if (provider != null) {
			getProvider().close();
		}
	}

	private RemoteHelperCommand lastCommand;

	public void handleLine(String line) throws Exception {
		if (provider == null) {
			throw new IllegalStateException("Remote Helper Provider was not set.");
		}

		line = line.trim();

		if (line.isEmpty()) {
			if (lastCommand != null) {
				lastCommand.complete();
				lastCommand = null;
			}

			return;
		}

		List<String> parts = Arrays.asList(line.split(" "));
		String commandName = parts.get(0);
		List<String> arguments;

		if (parts.size() > 1) {
			arguments = parts.subList(1, parts.size());
		} else {
			arguments = Collections.emptyList();
		}

		RemoteHelperCommand command = getProvider().getCommand(commandName);
		if (command == null) {
			command = getDefaultCommand(commandName);
		}

		if (command == null) {
			throw new IllegalArgumentException("Unknown Command: " + line);
		}

		command.handle(arguments);

		if (!command.isBatched()) {
			command.complete();
		} else {
			lastCommand = command;
		}
	}

	public RemoteHelperCommand getDefaultCommand(String name) {
		if ("capabilities".equals(name)) {
			return new DefaultCapabilitiesCommand(this);
		}
		return null;
	}

	private class DefaultCapabilitiesCommand extends RemoteHelperCommand {
		public DefaultCapabilitiesCommand(RemoteHelperContext context) {
			super(context);
		}

		@Override
		public void handle(List<String> arguments) throws Exception {
			for (RemoteHelperCapability capability : getProvider().getCapabilities()) {
				String line = capability.getName();
				if (capability.hasData()) {
					line += " " + capability.getData();
				}

				getContext().reply(line);
			}

			getContext().complete();
		}
	}
}
