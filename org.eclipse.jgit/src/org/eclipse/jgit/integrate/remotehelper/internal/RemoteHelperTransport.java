package org.eclipse.jgit.integrate.remotehelper.internal;

import org.eclipse.jgit.errors.NotSupportedException;
import org.eclipse.jgit.errors.TransportException;
import org.eclipse.jgit.integrate.remotehelper.RemoteHelperCapability;
import org.eclipse.jgit.integrate.remotehelper.RemoteHelperCommand;
import org.eclipse.jgit.integrate.remotehelper.RemoteHelperContext;
import org.eclipse.jgit.integrate.remotehelper.RemoteHelperProvider;
import org.eclipse.jgit.transport.FetchConnection;
import org.eclipse.jgit.transport.PushConnection;
import org.eclipse.jgit.transport.Transport;

import java.util.ArrayList;
import java.util.Collection;

public class RemoteHelperTransport extends RemoteHelperProvider {
	private Transport transport;

	public RemoteHelperTransport(RemoteHelperContext context) {
		super(context);

		Transport transport;
		try {
			transport = Transport.open(context.getRepository(), context.getUrl());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		setTransport(transport);
	}

	@Override
	public RemoteHelperCommand getCommand(String name) {
		if ("fetch".equals(name)) {
			return new TransportFetchCommand(this);
		} else if ("list".equals(name)) {
			return new TransportListCommand(this);
		} else if ("push".equals(name)) {
			return new TransportPushCommand(this);
		}
		return null;
	}

	@Override
	public Collection<RemoteHelperCapability> getCapabilities() {
		ArrayList<RemoteHelperCapability> capabilities = new ArrayList<>();

		try {
			FetchConnection fetchConnection = getFetchConnection();
			PushConnection pushConnection = getPushConnection();
			if (fetchConnection != null) {
				fetchConnection.close();
				capabilities.add(RemoteHelperCapability.FETCH);
			}

			if (pushConnection != null) {
				pushConnection.close();
				capabilities.add(RemoteHelperCapability.PUSH);
			}
		} catch (TransportException e) {
			throw new RuntimeException(e);
		}

		return capabilities;
	}

	public FetchConnection getFetchConnection() throws TransportException {
		try {
			return getTransport().openFetch();
		} catch (NotSupportedException e) {
			return null;
		}
	}

	public PushConnection getPushConnection() throws TransportException {
		try {
			return getTransport().openPush();
		} catch (NotSupportedException e) {
			return null;
		}
	}

	public void setTransport(Transport transport) {
		this.transport = transport;
	}

	public Transport getTransport() {
		if (transport == null) {
			throw new IllegalStateException("Transport was not initialized.");
		}
		return transport;
	}

	@Override
	public void close() {
		transport.close();
	}
}
