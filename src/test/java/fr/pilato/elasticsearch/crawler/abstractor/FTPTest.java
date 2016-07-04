/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package fr.pilato.elasticsearch.crawler.abstractor;

import fr.pilato.elasticsearch.crawler.fs.FsCrawlerImpl;
import fr.pilato.elasticsearch.crawler.fs.fileabstractor.FileAbstractModel;
import fr.pilato.elasticsearch.crawler.fs.fileabstractor.FileAbstractorFTP;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.FsSettings;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.mockftpserver.fake.filesystem.WindowsFakeFileSystem;

import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class FTPTest {

    protected static final Logger logger = LogManager.getLogger(FTPTest.class);

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

    @Test
    public void testFtp() throws Exception {
        String username = "user";
        String password = "password";
        String hostname = "127.0.0.1";
        String dir = "/test_ftp";
        String subdir = dir + "/subdir/";

        Server server = Server.builder()
                .setHostname(hostname)
                .setUsername(username)
                .setPassword(password)
                .setProtocol(FsCrawlerImpl.PROTOCOL.FTP)
                .setPort(fakeFtpServer.getServerControlPort())
                .build();
        FsSettings settings = FsSettings.builder("test").setServer(server).build();
        FileAbstractorFTP ftp = new FileAbstractorFTP(settings);

        ftp.open();
        assertThat(ftp.exists("doesnotexists"), is(false));
        assertThat(ftp.exists(dir), is(true));
        Collection<FileAbstractModel> files = ftp.getFiles(dir);
        for (FileAbstractModel file : files) {
            logger.info("{}", file.name);
        }
        assertThat(ftp.exists(subdir), is(true));
        files = ftp.getFiles(subdir);
        for (FileAbstractModel file : files) {
            logger.info("{}", file.name);
        }
        ftp.close();
    }
}
