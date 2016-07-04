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
import fr.pilato.elasticsearch.crawler.fs.meta.job.FsJobFileHandler;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Elasticsearch;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Fs;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.FsSettings;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.FsSettingsFileHandler;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.FsSettingsParser;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Server;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.TimeValue;
import fr.pilato.elasticsearch.crawler.fs.util.FsCrawlerUtil;
import fr.pilato.elasticsearch.crawler.util.InternalFileVisitor;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test all crawler settings
 */
public abstract class AbstractFsCrawlerImplAllParametersTest extends AbstractMonoNodeITest {

    protected FsCrawlerImpl crawler = null;
    protected Path currentTestResourceDir;
    protected Path metadataDir;

    /**
     * We suppose that each test has its own set of files. Even if we duplicate them, that will make the code
     * more readable.
     * The temp folder which is used as a root is automatically cleaned after the test so we don't have to worry
     * about it.
     */
    @Before
    public void copyTestResources() throws IOException {
        Path testResourceTarget = Paths.get(folder.getRoot().toURI()).resolve("resources");
        if (Files.notExists(testResourceTarget)) {
            Files.createDirectory(testResourceTarget);
        }
        metadataDir = Paths.get(folder.getRoot().toURI()).resolve(".fscrawler");
        if (Files.notExists(metadataDir)) {
            Files.createDirectory(metadataDir);
        }

        String currentTestName = getCurrentTestName();
        // We copy files from the src dir to the temp dir
        staticLogger.info("  --> Launching test [{}]", currentTestName);
        String url = getUrl(currentTestName);
        Path from = Paths.get(url);
        currentTestResourceDir = testResourceTarget.resolve(currentTestName);

        staticLogger.info("  --> Copying test resources from [{}]", from);
        if (Files.notExists(from)) {
            logger.error("directory [{}] should be copied to [{}]", from, currentTestResourceDir);
            throw new RuntimeException(from + " doesn't seem to exist. Check your JUnit tests.");
        }

        Files.walkFileTree(from, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
                new InternalFileVisitor(from, currentTestResourceDir));

        staticLogger.info("  --> Test resources ready in [{}]", currentTestResourceDir);
    }

    @After
    public void shutdownCrawler() {
        stopCrawler();
    }


    private static final String testCrawlerPrefix = "fscrawler_";

    protected String getCrawlerName() {
        String testName = testCrawlerPrefix.concat(getCurrentTestName());
        return testName.contains(" ") ? Strings.split(testName, " ")[0] : testName;
    }

    protected Fs.Builder startCrawlerDefinition() throws IOException {
        return startCrawlerDefinition(currentTestResourceDir.toString(), TimeValue.timeValueSeconds(5));
    }

    protected Fs.Builder startCrawlerDefinition(TimeValue updateRate) throws IOException {
        return startCrawlerDefinition(currentTestResourceDir.toString(), updateRate);
    }

    protected Fs.Builder startCrawlerDefinition(String dir) throws IOException {
        return startCrawlerDefinition(dir, TimeValue.timeValueSeconds(5));
    }

    protected Fs.Builder startCrawlerDefinition(String dir, TimeValue updateRate) {
        logger.info("  --> creating crawler for dir [{}]", dir);
        return Fs
                .builder()
                .setUrl(dir)
                .setUpdateRate(updateRate);
    }

    protected Elasticsearch endCrawlerDefinition(String indexName) {
        return Elasticsearch.builder()
                .setIndex(indexName)
                .addNode(Elasticsearch.Node.builder().setHost("127.0.0.1").setPort(HTTP_TEST_PORT).build())
                .setBulkSize(1)
                .build();
    }

    public static File URItoFile(URL url) {
        try {
            return new File(url.toURI());
        } catch(URISyntaxException e) {
            return new File(url.getPath());
        }
    }

    public static String getUrl(String dir) {
        URL resource = AbstractFsCrawlerImplAllParametersTest.class.getResource("/job-sample.json");
        File resourceDir = new File(URItoFile(resource).getParentFile(), "samples");
        File dataDir = new File(resourceDir, dir);

        return dataDir.getAbsoluteFile().getAbsolutePath();
    }

    protected void startCrawler() throws Exception {
        startCrawler(getCrawlerName());
    }

    protected void startCrawler(final String jobName) throws Exception {
        startCrawler(jobName, startCrawlerDefinition().build(), endCrawlerDefinition(jobName), null);
    }

    protected void startCrawler(final String jobName, Fs fs, Elasticsearch elasticsearch, Server server) throws Exception {
        logger.info("  --> starting crawler [{}]", jobName);

        // TODO do this rarely() createIndex(jobName);

        crawler = new FsCrawlerImpl(metadataDir, FsSettings.builder(jobName).setElasticsearch(elasticsearch).setFs(fs).setServer(server).build());
        crawler.start();

        // We wait up to 10 seconds before considering a failing test
        assertThat("Job meta file should exists in ~/.fscrawler...", awaitBusy(() -> {
            try {
                new FsJobFileHandler(metadataDir).read(jobName);
                return true;
            } catch (IOException e) {
                return false;
            }
        }), equalTo(true));

        countTestHelper(jobName, null, null);

        // Make sure we refresh indexed docs before launching tests
        refresh();

        // Print crawler settings
        FsSettings fsSettings = new FsSettingsFileHandler(metadataDir).read(jobName);
        logger.info("  --> Index settings [{}]", FsSettingsParser.toJson(fsSettings));
    }

    protected void refresh() {
        client.admin().indices().prepareRefresh().get();
    }

    protected void createIndex(String index) {
        logger.info("  --> createIndex({})", index);
        client.admin().indices().prepareCreate(index).get();
    }

    protected void stopCrawler() {
        logger.info("  --> stopping all crawlers");
        if (crawler != null) {
            staticLogger.info("  --> Stopping crawler");
            crawler.close();
            crawler = null;
        }
    }

    /**
     * Check that we have the expected number of docs or at least one if expected is null
     *
     * @param indexName Index we will search in.
     * @param term      Term you search for. MatchAll if null.
     * @param expected  expected number of docs. Null if at least 1.
     * @return the search response if further tests are needed
     * @throws Exception
     */
    public SearchResponse countTestHelper(final String indexName, String term, final Integer expected) throws Exception {
        return countTestHelper(indexName, term, expected, null, null);
    }

    /**
     * Check that we have the expected number of docs or at least one if expected is null
     *
     * @param indexName Index we will search in.
     * @param term      Term you search for. MatchAll if null.
     * @param expected  expected number of docs. Null if at least 1.
     * @param path      Path we are supposed to scan. If we have not accurate results, we display its content
     * @param esQuery   If we want to use a specific elasticsearch query for a test instead of queryStringQuery or MathAll
     * @param fields    If we want to add some fields within the response
     * @return the search response if further tests are needed
     * @throws Exception
     */
    public SearchResponse countTestHelper(final String indexName, String term, final Integer expected, final Path path,
                                          final QueryBuilder esQuery, final String... fields) throws Exception {
        // Let's search for entries
        final QueryBuilder query;
        if (esQuery != null) {
            query = esQuery;
        } else {
            if (term == null) {
                query = QueryBuilders.matchAllQuery();
            } else {
                query = QueryBuilders.queryStringQuery(term);
            }
        }

        final SearchResponse[] response = new SearchResponse[1];

        // We wait up to 5 seconds before considering a failing test
        assertThat("We waited for 5 seconds but not enough documents have been added. Expected: " +
                (expected == null ? "at least one" : expected), awaitBusy(() -> {
            long totalHits;
            SearchRequestBuilder requestBuilder = client.prepareSearch(indexName)
                    .setTypes(FsCrawlerUtil.INDEX_TYPE_DOC)
                    .setQuery(query);
            for (String field : fields) {
                requestBuilder.addField(field);
            }
            response[0] = requestBuilder.get();
            logger.debug("result {}", response[0].toString());
            totalHits = response[0].getHits().getTotalHits();

            if (expected == null) {
                return (totalHits >= 1);
            } else {
                if (expected == totalHits) {
                    return true;
                } else {
                    logger.info("     ---> expecting [{}] but got [{}] documents in [{}]", expected, totalHits, indexName);
                    if (path != null) {
                        logger.info("     ---> content of [{}]:", path);
                        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path)) {
                            for (Path file : directoryStream) {
                                logger.info("         - {} {}",
                                        file.getFileName().toString(),
                                        Files.getLastModifiedTime(file));
                            }
                        } catch (IOException ex) {
                            logger.error("can not read content of [{}]:", path);
                        }
                    }
                    return false;
                }
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));

        return response[0];
    }

}
