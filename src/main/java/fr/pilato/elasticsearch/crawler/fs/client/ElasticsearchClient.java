package fr.pilato.elasticsearch.crawler.fs.client;

import com.fasterxml.jackson.databind.node.ObjectNode;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Elasticsearch;

import java.io.IOException;

public interface ElasticsearchClient {
    void createIndex(String index) throws IOException;

    void createIndex(String index, boolean ignoreErrors) throws IOException;

    BulkResponse bulk(BulkRequest bulkRequest) throws Exception;

    void putMapping(String index, String type, ObjectNode mapping) throws Exception;

    SearchResponse search(String index, String type, String query) throws IOException;

    SearchResponse search(String index, String type, String query, Integer size, String field) throws IOException;

    SearchResponse search(String index, String type, SearchRequest searchRequest) throws IOException;

    boolean isExistingType(String index, String type) throws IOException;
}
