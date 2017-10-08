package org.eclipse.jgit.integrate.remotehelper;

public class RemoteHelperCapability {
	public static final RemoteHelperCapability CONNECT = new RemoteHelperCapability("connect");
	public static final RemoteHelperCapability PUSH = new RemoteHelperCapability("push");
	public static final RemoteHelperCapability EXPORT = new RemoteHelperCapability("export");
	public static final RemoteHelperCapability FETCH = new RemoteHelperCapability("fetch");
	public static final RemoteHelperCapability IMPORT = new RemoteHelperCapability("import");
	public static final RemoteHelperCapability OPTION = new RemoteHelperCapability("option");
	public static final RemoteHelperCapability BIDI_IMPORT = new RemoteHelperCapability("bidi-import");


	private final String name;
	private final String data;

	public RemoteHelperCapability(String name) {
		this(name, null);
	}

	public RemoteHelperCapability(String name, String data) {
		this.name = name;
		this.data = data;
	}

	public String getName() {
		return name;
	}

	public String getData() {
		return data;
	}

	public boolean hasData() {
		return data != null;
	}
}
