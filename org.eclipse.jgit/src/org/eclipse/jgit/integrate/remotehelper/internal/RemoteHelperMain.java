package org.eclipse.jgit.integrate.remotehelper.internal;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.integrate.remotehelper.RemoteHelperDriver;
import org.eclipse.jgit.lib.TextProgressMonitor;
import org.eclipse.jgit.transport.URIish;

import java.io.*;

public class RemoteHelperMain {
	public static void main(String[] args) throws Exception {
		main(args, System.getenv("GIT_DIR"));
	}

	public static void main(String[] args, String gitDir) throws Exception {
		if (args.length < 1 || args.length > 2) {
			System.err.println("Usage: remote-helper <remote/url> [url]");
			System.exit(1);
		}

		String remoteName = args.length == 2 ? args[0] : "";
		String url = args.length == 2 ? args[1] : args[0];

		if (gitDir == null) {
			System.err.println("ERROR: GIT_DIR was not specified.");
			System.exit(1);
		}

		RemoteHelperDriver driver = new RemoteHelperDriver(
			Git.open(new File(gitDir)).getRepository(),
			new URIish(url),
			remoteName,
			new PrintWriter(System.out),
			new TextProgressMonitor()
		);

		driver.setProvider(new RemoteHelperTransport(driver));

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

		String line;
		while ((line = reader.readLine()) != null) {
			driver.handleLine(line);
		}
	}
}
