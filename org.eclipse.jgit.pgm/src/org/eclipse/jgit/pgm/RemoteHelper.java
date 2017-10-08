/*
 * Copyright (C) 2009, Constantine Plotnikov <constantine.plotnikov@gmail.com>
 * Copyright (C) 2008, Google Inc.
 * Copyright (C) 2010, Robin Rosenberg <robin.rosenberg@dewire.com>
 * Copyright (C) 2010, Sasa Zivkov <sasa.zivkov@sap.com>
 * Copyright (C) 2010, Chris Aniszczyk <caniszczyk@gmail.com>
 * Copyright (C) 2016, Rüdiger Herrmann <ruediger.herrmann@gmx.de>
 * and other copyright owners as documented in the project's IP log.
 *
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Distribution License v1.0 which
 * accompanies this distribution, is reproduced below, and is
 * available at http://www.eclipse.org/org/documents/edl-v10.php
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 *
 * - Neither the name of the Eclipse Foundation, Inc. nor the
 *   names of its contributors may be used to endorse or promote
 *   products derived from this software without specific prior
 *   written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.eclipse.jgit.pgm;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.integrate.remotehelper.RemoteHelperDriver;
import org.eclipse.jgit.integrate.remotehelper.internal.RemoteHelperMain;
import org.eclipse.jgit.integrate.remotehelper.internal.RemoteHelperTransport;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TextProgressMonitor;
import org.eclipse.jgit.transport.URIish;
import org.kohsuke.args4j.Argument;

import java.io.*;

@Command(common = true, name = "remote-helper", usage = "usage_RemoteHelper")
class RemoteHelper extends TextBuiltin {
	@Argument(index = 0, metaVar = "metaVar_remoteName")
	private String remote;

	@Argument(index = 1, metaVar = "metaVar_url")
	private String url;

	@Override
	protected final boolean requiresRepository() {
		return false;
	}

	@Override
	protected void run() throws Exception {
		String realUrl = url == null ? remote : url;
		String realRemote = url == null ? "" : remote;

		String dir = gitdir;
		if (dir == null) {
			dir = System.getenv("GIT_DIR");
		}

		if (dir == null) {
			dir = ".";
		}

		Repository repository = Git.open(new File(dir)).getRepository();
		init(repository, null);

		RemoteHelperDriver driver = new RemoteHelperDriver(
			repository,
			new URIish(realUrl),
			realRemote,
			new PrintWriter(outw),
			new TextProgressMonitor(errw)
		);

		driver.setProvider(new RemoteHelperTransport(driver));

		BufferedReader reader = new BufferedReader(new InputStreamReader(ins));

		String line;
		while ((line = reader.readLine()) != null) {
			driver.handleLine(line);
		}
	}
}
