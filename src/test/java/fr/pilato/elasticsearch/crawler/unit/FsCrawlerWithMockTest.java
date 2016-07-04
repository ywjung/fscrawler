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

package fr.pilato.elasticsearch.crawler.unit;

import com.fasterxml.jackson.databind.node.ObjectNode;
import fr.pilato.elasticsearch.crawler.fs.AbstractFSCrawlerTest;
import fr.pilato.elasticsearch.crawler.fs.FSsCrawlerListener;
import fr.pilato.elasticsearch.crawler.fs.FsCrawlerImpl;
import fr.pilato.elasticsearch.crawler.fs.client.BulkRequest;
import fr.pilato.elasticsearch.crawler.fs.client.BulkResponse;
import fr.pilato.elasticsearch.crawler.fs.client.ElasticsearchClient;
import fr.pilato.elasticsearch.crawler.fs.client.SearchRequest;
import fr.pilato.elasticsearch.crawler.fs.client.SearchResponse;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Fs;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.FsSettings;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FsCrawlerWithMockTest extends AbstractFSCrawlerTest {
    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    protected static final Logger staticLogger = LogManager.getLogger(FsCrawlerWithMockTest.class);

    protected FakeFtpServer fakeFtpServer;
    protected Path metadataDir;

    /**
     * We suppose that each test has its own set of files. Even if we duplicate them, that will make the code
     * more readable.
     * The temp folder which is used as a root is automatically cleaned after the test so we don't have to worry
     * about it.
     */
    @Before
    public void copyTestResources() throws IOException {
        metadataDir = Paths.get(folder.getRoot().toURI()).resolve(".fscrawler");
        if (Files.notExists(metadataDir)) {
            Files.createDirectory(metadataDir);
        }

        String currentTestName = getCurrentTestName();
        // We copy files from the src dir to the temp dir
        staticLogger.info("  --> Launching test [{}]", currentTestName);
    }

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
    public void testStart() throws Exception {
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

        FsSettings settings = FsSettings.builder("test")
                .setServer(server)
                .setFs(Fs.builder().setUrl(dir).build())
                .build();

        FsCrawlerImpl fsCrawler = new FsCrawlerImpl(metadataDir, settings);
        fsCrawler.start(new FSsCrawlerListener() {
            @Override
            public void esIndex(boolean isClosed, String index, String type, String id, String json) {
                logger.info("MOCK: indexing in elasticsearch {}/{}/{}", index, type, id);
            }

            @Override
            public void esDelete(boolean isClosed, String index, String type, String id) {
                logger.info("MOCK: removing from elasticsearch {}/{}/{}", index, type, id);
            }
        }, new ElasticsearchClient() {
            @Override
            public void createIndex(String index) throws IOException {
                logger.info("MOCK: create index {}", index);
            }

            @Override
            public void createIndex(String index, boolean ignoreErrors) throws IOException {
                logger.info("MOCK: create index {}, {}", index, ignoreErrors);
            }

            @Override
            public BulkResponse bulk(BulkRequest bulkRequest) throws Exception {
                return null;
            }

            @Override
            public void putMapping(String index, String type, ObjectNode mapping) throws Exception {

            }

            @Override
            public SearchResponse search(String index, String type, String query) throws IOException {
                return null;
            }

            @Override
            public SearchResponse search(String index, String type, String query, Integer size, String field) throws IOException {
                return new SearchResponse();
            }

            @Override
            public SearchResponse search(String index, String type, SearchRequest searchRequest) throws IOException {
                return null;
            }

            @Override
            public boolean isExistingType(String index, String type) throws IOException {
                return false;
            }
        });

        Thread.sleep(5000);

    }
}
