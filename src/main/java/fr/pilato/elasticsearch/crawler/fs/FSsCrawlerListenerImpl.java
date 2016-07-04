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

package fr.pilato.elasticsearch.crawler.fs;

import fr.pilato.elasticsearch.crawler.fs.client.BulkProcessor;
import fr.pilato.elasticsearch.crawler.fs.client.DeleteRequest;
import fr.pilato.elasticsearch.crawler.fs.client.IndexRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FSsCrawlerListenerImpl implements FSsCrawlerListener {

    private static final Logger logger = LogManager.getLogger(FSsCrawlerListenerImpl.class);
    private final BulkProcessor bulkProcessor;

    public FSsCrawlerListenerImpl(BulkProcessor bulkProcessor) {
        this.bulkProcessor = bulkProcessor;
    }

    @Override
    public void esIndex(boolean isClosed, String index, String type, String id, String json) {
        logger.debug("Indexing in ES " + index + ", " + type + ", " + id);
        logger.trace("JSon indexed : {}", json);

        if (!isClosed) {
            bulkProcessor.add(new IndexRequest(index, type, id).source(json));
        } else {
            logger.warn("trying to add new file while closing crawler. Document [{}]/[{}]/[{}] has been ignored", index, type, id);
        }
    }

    @Override
    public void esDelete(boolean isClosed, String index, String type, String id) {
        logger.debug("Deleting from ES " + index + ", " + type + ", " + id);
        if (!isClosed) {
            bulkProcessor.add(new DeleteRequest(index, type, id));
        } else {
            logger.warn("trying to remove a file while closing crawler. Document [{}]/[{}]/[{}] has been ignored", index, type, id);
        }
    }
}
