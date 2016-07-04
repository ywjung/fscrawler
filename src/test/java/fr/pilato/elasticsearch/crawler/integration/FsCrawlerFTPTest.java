/*
 * Licensed to David Pilato (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package fr.pilato.elasticsearch.crawler.integration;

import fr.pilato.elasticsearch.crawler.fs.FsCrawlerImpl;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Fs;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

/**
 * Test all crawler settings
 */
public class FsCrawlerFTPTest extends AbstractFsCrawlerImplAllParametersTest {

    protected FakeFtpServer fakeFtpServer;

    @Before
    public void startFTPServer() {
        fakeFtpServer = new FakeFtpServer();
        // We let the FTPServer to find a free port
        fakeFtpServer.setServerControlPort(0);
        fakeFtpServer.addUserAccount(new UserAccount("user", "password", "/"));

        FileSystem fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new DirectoryEntry("/test_ftp"));
        fileSystem.add(new FileEntry("/test_ftp/rootfile.txt", "This file contains some words.\n"));
        fileSystem.add(new DirectoryEntry("/test_ftp/subdir"));
        fileSystem.add(new FileEntry("/test_ftp/subdir/roottxtfile_multi_feed.txt", "This file contains some words. This testcase is used" +
                " in multi feed crawlers !\n"));
        fakeFtpServer.setFileSystem(fileSystem);

        fakeFtpServer.start();
    }

    @After
    public void stopFTPServer() {
        if (fakeFtpServer != null) {
            fakeFtpServer.stop();
        }
    }


    /**
     * You have to adapt this test to your own system (login / password and FTP connexion)
     * So this test is disabled by default
     */
    @Test
    public void test_ftp() throws Exception {
        String username = "user";
        String password = "password";
        String hostname = "127.0.0.1";

        Fs fs = startCrawlerDefinition("/test_ftp").build();
        Server server = Server.builder()
                .setHostname(hostname)
                .setUsername(username)
                .setPassword(password)
                .setProtocol(FsCrawlerImpl.PROTOCOL.FTP)
                .setPort(fakeFtpServer.getServerControlPort())
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), server);

        countTestHelper(getCrawlerName(), null, 2);
    }
}
