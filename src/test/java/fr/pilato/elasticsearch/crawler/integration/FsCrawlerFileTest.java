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

import fr.pilato.elasticsearch.crawler.fs.client.ElasticsearchClientImpl;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Elasticsearch;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Fs;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Percentage;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.TimeValue;
import fr.pilato.elasticsearch.crawler.fs.util.FsCrawlerUtil;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test all crawler settings
 */
public class FsCrawlerFileTest extends AbstractFsCrawlerImplAllParametersTest {

    @Test
    public void test_default_settings() throws Exception {
        ElasticsearchClientImpl client = ElasticsearchClientImpl.builder().build();
        boolean active = client.isActive(Elasticsearch.DEFAULT.getNodes().get(0));
        if (active) {
            logger.warn("you have a local elasticsearch node running on 9200 port. We will skip the test.");
            return;
        }

        try {
            startCrawler(getCrawlerName(), null, null, null);
        } catch (IOException e) {
            // We expect it as we probably don't have an elasticsearch node running with default 9200 port
        }
    }

    @Test
    public void test_filesize() throws Exception {
        startCrawler();

        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1);
        for (SearchHit hit : searchResponse.getHits()) {
            Map<String, Object> file = (Map<String, Object>) hit.getSource().get(FsCrawlerUtil.Doc.FILE);
            assertThat(file, notNullValue());
            assertThat(file.get(FsCrawlerUtil.Doc.File.FILESIZE), is(30));
        }
    }

    @Test
    public void test_filesize_limit() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setIndexedChars(new Percentage(7))
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1, null, null, "*");
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.CONTENT), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.INDEXED_CHARS), notNullValue());

            // Our original text: "Bonjour David..." should be truncated
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.CONTENT).getValue(), is("Bonjour"));
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.INDEXED_CHARS).getValue(), is(7L));
        }
    }

    @Test
    public void test_filesize_limit_percentage() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setIndexedChars(Percentage.parse("0.1%"))
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1, null, null, "*");
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.CONTENT), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.INDEXED_CHARS), notNullValue());

            // Our original text: "Bonjour David..." should be truncated
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.CONTENT).getValue(), is("Bonjour "));
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.INDEXED_CHARS).getValue(), is(8L));
        }
    }

    @Test
    public void test_filesize_nolimit() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setIndexedChars(new Percentage(-1))
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1, null, null, "*");
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.CONTENT), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.INDEXED_CHARS), nullValue());

            // Our original text: "Bonjour David\n\n\n" should not be truncated
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.CONTENT).getValue(), is("Bonjour David\n\n\n"));
        }
    }

    @Test
    public void test_filesize_disabled() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setAddFilesize(false)
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1);
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getFields().get("filesize"), nullValue());
        }
    }

    @Test
    public void test_includes() throws Exception {
        Fs fs = startCrawlerDefinition()
                .addInclude("*_include.txt")
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);
        countTestHelper(getCrawlerName(), null, 1);
    }

    @Test
    public void test_metadata() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setIndexedChars(new Percentage(1))
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1, null, null, "*");
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.FILENAME), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.CONTENT_TYPE), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.URL), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.FILESIZE), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.INDEXING_DATE), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.INDEXED_CHARS), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.LAST_MODIFIED), notNullValue());

            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.META + "." + FsCrawlerUtil.Doc.Meta.TITLE), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.META + "." + FsCrawlerUtil.Doc.Meta.KEYWORDS), notNullValue());
        }
    }

    @Test
    public void test_default_metadata() throws Exception {
        startCrawler();

        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1, null, null, "*");
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.ATTACHMENT), nullValue());

            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.FILENAME), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.CONTENT_TYPE), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.URL), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.FILESIZE), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.INDEXING_DATE), notNullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.INDEXED_CHARS), nullValue());
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.FILE + "." + FsCrawlerUtil.Doc.File.LAST_MODIFIED), notNullValue());

            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.META + "." + FsCrawlerUtil.Doc.Meta.TITLE), notNullValue());
        }
    }

    @Test
    public void test_attributes() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setAttributesSupport(true)
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);
        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1, null, null, "attributes.owner");
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.ATTRIBUTES + "." + FsCrawlerUtil.Doc.Attributes.OWNER), notNullValue());
        }
    }

    @Test
    public void test_remove_deleted_enabled() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setRemoveDeleted(true)
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        // We should have two docs first
        countTestHelper(getCrawlerName(), null, 2, currentTestResourceDir, null);

        // We remove a file
        logger.info(" ---> Removing file deleted_roottxtfile.txt");
        Files.delete(currentTestResourceDir.resolve("deleted_roottxtfile.txt"));

        // We expect to have two files
        countTestHelper(getCrawlerName(), null, 1, currentTestResourceDir, null);
    }

    @Test
    public void test_remove_deleted_disabled() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setRemoveDeleted(false)
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        // We should have two docs first
        countTestHelper(getCrawlerName(), null, 2, currentTestResourceDir, null);

        // We remove a file
        logger.info(" ---> Removing file deleted_roottxtfile.txt");
        Files.delete(currentTestResourceDir.resolve("deleted_roottxtfile.txt"));

        // We expect to have two files
        countTestHelper(getCrawlerName(), null, 2, currentTestResourceDir, null);
    }

    /**
     * Test case for https://github.com/dadoonet/fscrawler/issues/110
     * @throws Exception In case something is wrong
     */
    @Test
    public void test_rename_file() throws Exception {
        startCrawler();

        // We should have two docs first
        countTestHelper(getCrawlerName(), null, 1, currentTestResourceDir, null);

        // We rename the file
        logger.info(" ---> Renaming file roottxtfile.txt to renamed_roottxtfile.txt");
        // We create a copy of a file
        Files.move(currentTestResourceDir.resolve("roottxtfile.txt"),
                currentTestResourceDir.resolve("renamed_roottxtfile.txt"));

        // We expect to have one file only with a new name
        countTestHelper(getCrawlerName(), "file.filename:renamed_roottxtfile.txt", 1, currentTestResourceDir, null);
    }

    /**
     * Test case for issue #60: https://github.com/dadoonet/fscrawler/issues/60 : new files are not added
     */
    @Test
    public void test_add_new_file() throws Exception {
        startCrawler();

        // We should have one doc first
        countTestHelper(getCrawlerName(), null, 1, currentTestResourceDir, null);

        logger.info(" ---> Adding a copy of roottxtfile.txt");
        // We create a copy of a file
        Files.copy(currentTestResourceDir.resolve("roottxtfile.txt"),
                currentTestResourceDir.resolve("new_roottxtfile.txt"));

        // We expect to have two files
        countTestHelper(getCrawlerName(), null, 2, currentTestResourceDir, null);
    }

    /**
     * Test case for issue #5: https://github.com/dadoonet/fscrawler/issues/5 : Support JSon documents
     */
    @Test
    public void test_json_support() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setJsonSupport(true)
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        assertThat("We should have 0 doc for tweet in text field...", awaitBusy(() -> {
            SearchResponse searchResponse = client.prepareSearch(getCrawlerName())
                    .setQuery(QueryBuilders.termQuery("text", "tweet")).get();
            return searchResponse.getHits().getTotalHits() == 2;
        }), equalTo(true));
    }

    /**
     * Test case for issue #5: https://github.com/dadoonet/fscrawler/issues/5 : Support JSon documents
     */
    @Test
    public void test_json_disabled() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setJsonSupport(false)
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        assertThat("We should have 0 doc for tweet in text field...", awaitBusy(() -> {
            SearchResponse searchResponse = client.prepareSearch(getCrawlerName())
                    .setQuery(QueryBuilders.termQuery("text", "tweet")).get();
            return searchResponse.getHits().getTotalHits() == 0;
        }), equalTo(true));

        assertThat("We should have 2 docs for tweet in _all...", awaitBusy(() -> {
            SearchResponse searchResponse = client.prepareSearch(getCrawlerName())
                    .setQuery(QueryBuilders.queryStringQuery("tweet")).get();
            return searchResponse.getHits().getTotalHits() == 2;
        }), equalTo(true));
    }

    /**
     * Test case for issue #7: https://github.com/dadoonet/fscrawler/issues/7 : JSON support: use filename as ID
     */
    @Test
    public void test_filename_as_id() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setJsonSupport(true)
                .setFilenameAsId(true)
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        assertThat("Document should exists with [tweet1] id...", awaitBusy(() -> {
            GetResponse getResponse = client.prepareGet(getCrawlerName(), FsCrawlerUtil.INDEX_TYPE_DOC, "tweet1").get();
            return getResponse.isExists();
        }), equalTo(true));

        assertThat("Document should exists with [tweet2] id...", awaitBusy(() -> {
            GetResponse getResponse = client.prepareGet(getCrawlerName(), FsCrawlerUtil.INDEX_TYPE_DOC, "tweet2").get();
            return getResponse.isExists();
        }), equalTo(true));
    }

    @Test
    public void test_store_source() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setStoreSource(true)
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1, null, null, "_source", "*");
        for (SearchHit hit : searchResponse.getHits()) {
            // We check that the field has been stored
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.ATTACHMENT), notNullValue());

            // We check that the field is not part of _source
            assertThat(hit.getSource().get(FsCrawlerUtil.Doc.ATTACHMENT), nullValue());
        }
    }

    @Test
    public void test_do_not_store_source() throws Exception {
        startCrawler();

        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1, null, null, "_source", "*");
        for (SearchHit hit : searchResponse.getHits()) {
            // We check that the field has not been stored
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.ATTACHMENT), nullValue());

            // We check that the field is not part of _source
            assertThat(hit.getSource().get(FsCrawlerUtil.Doc.ATTACHMENT), nullValue());
        }
    }

    @Test
    public void test_defaults() throws Exception {
        startCrawler();

        // We expect to have one file
        SearchResponse searchResponse = countTestHelper(getCrawlerName(), null, 1);

        // The default configuration should not add file attributes
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getFields().get(FsCrawlerUtil.Doc.ATTRIBUTES + "." + FsCrawlerUtil.Doc.Attributes.OWNER), nullValue());
        }

    }

    @Test
    public void test_subdirs() throws Exception {
        startCrawler();

        // We expect to have two files
        countTestHelper(getCrawlerName(), null, 2);
    }

    @Test
    public void test_ignore_dir() throws Exception {
        Fs fs = startCrawlerDefinition()
                .addExclude(".ignore")
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        // We expect to have one file
        countTestHelper(getCrawlerName(), null, 1);
    }

    @Test
    public void test_multiple_crawlers() throws Exception {
        Fs fs1 = startCrawlerDefinition(currentTestResourceDir.resolve("crawler1").toString()).build();
        Fs fs2 = startCrawlerDefinition(currentTestResourceDir.resolve("crawler2").toString()).build();
        startCrawler(getCrawlerName() + "_1", fs1, endCrawlerDefinition(getCrawlerName() + "_1"), null);
        startCrawler(getCrawlerName() + "_2", fs2, endCrawlerDefinition(getCrawlerName() + "_2"), null);
        // We should have one doc in index 1...
        countTestHelper(getCrawlerName() + "_1", null, 1);
        // We should have one doc in index 2...
        countTestHelper(getCrawlerName() + "_2", null, 1);
    }

    @Test
    public void test_filename_analyzer() throws Exception {
        startCrawler();

        // We should have one doc
        countTestHelper(getCrawlerName(), null, 1, null, QueryBuilders.termQuery("file.filename", "roottxtfile.txt"));
    }

    /**
     * Test for #83: https://github.com/dadoonet/fscrawler/issues/83
     */
    @Test
    public void test_time_value() throws Exception {
        Fs fs = startCrawlerDefinition(TimeValue.timeValueHours(1)).build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        // We expect to have one file
        countTestHelper(getCrawlerName(), null, 1);
    }

    /**
     * Test for #87: https://github.com/dadoonet/fscrawler/issues/87
     */
    @Test
    public void test_mp3() throws Exception {
        Fs fs = startCrawlerDefinition(TimeValue.timeValueHours(1)).build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        // We expect to have one file
        countTestHelper(getCrawlerName(), null, 1);
    }

    /**
     * Test for #105: https://github.com/dadoonet/fscrawler/issues/105
     */
    @Test
    public void test_unparsable() throws Exception {
        startCrawler();

        // We expect to have two files
        countTestHelper(getCrawlerName(), null, 2);
    }

    /**
     * Test for #103: https://github.com/dadoonet/fscrawler/issues/103
     */
    @Test
    public void test_index_content() throws Exception {
        Fs fs = startCrawlerDefinition()
                .setIndexContent(false)
                .build();
        startCrawler(getCrawlerName(), fs, endCrawlerDefinition(getCrawlerName()), null);

        // We expect to have one file
        countTestHelper(getCrawlerName(), null, 1);

        countTestHelper(getCrawlerName(), null, 0, null, QueryBuilders.prefixQuery("content", "file"));
        countTestHelper(getCrawlerName(), null, 0, null, QueryBuilders.prefixQuery("file.content_type", "text/plain"));
    }

    @Test
    public void test_bulk_flush() throws Exception {
        Fs fs = startCrawlerDefinition().build();
        startCrawler(getCrawlerName(), fs, Elasticsearch.builder()
                .setIndex(getCrawlerName())
                .addNode(Elasticsearch.Node.builder().setHost("127.0.0.1").setPort(HTTP_TEST_PORT).build())
                .setBulkSize(100)
                .setFlushInterval(TimeValue.timeValueSeconds(2))
                .build(), null);

        countTestHelper(getCrawlerName(), null, 1);
    }

    /**
     * Test for word document
     */
    @Test
    public void test_word() throws Exception {
        startCrawler();

        // We expect to have two files
        countTestHelper(getCrawlerName(), "figure", 1);
    }

}
