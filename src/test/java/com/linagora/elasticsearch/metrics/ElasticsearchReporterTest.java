/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.linagora.elasticsearch.metrics;


import static com.codahale.metrics.MetricRegistry.name;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linagora.elasticsearch.metrics.percolation.Notifier;

public class ElasticsearchReporterTest extends ESIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueMillis(1000L);

    private MetricRegistry registry = new MetricRegistry();
    private String index = randomAsciiOfLength(12).toLowerCase();
    private String indexWithDate = String.format("%s-%s-%02d", index, Calendar.getInstance().get(Calendar.YEAR), Calendar.getInstance().get(Calendar.MONTH) + 1);
    private String prefix = randomAsciiOfLength(12).toLowerCase();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.HTTP_ENABLED, true)
            .build();
    }


    @Test
    public void testThatTemplateIsAdded() throws IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates("metrics_template").get(TIMEOUT);

            assertThat(response.getIndexTemplates(), hasSize(1));
            IndexTemplateMetaData templateData = response.getIndexTemplates().get(0);
            assertThat(templateData.order(), is(0));
            assertThat(templateData.getMappings().get("_default_"), is(notNullValue()));
        }
    }

    @Test
    public void testThatMappingFromTemplateIsApplied() throws Exception {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            registry.counter(name("test", "cache-evictions")).inc();
            reportAndRefresh(elasticsearchReporter);
            ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setRoutingTable(false)
                .setLocal(false)
                .setNodes(true)
                .setIndices(indexWithDate)
                .execute().actionGet(TIMEOUT);

            assertThat(clusterStateResponse.getState().getMetaData().getIndices().containsKey(indexWithDate), is(true));
            IndexMetaData indexMetaData = clusterStateResponse.getState().getMetaData().getIndices().get(indexWithDate);
            assertThat(indexMetaData.getMappings().containsKey("metrics"), is(true));
            Map<String, Object> properties = getAsMap(indexMetaData.mapping("metrics").sourceAsMap(), "properties");
            Map<String, Object> mapping = getAsMap(properties, "name");
            assertThat(mapping, hasKey("index"));
            assertThat(mapping.get("index").toString(), is("not_analyzed"));
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getAsMap(Map<String, Object> map, String key) {
        assertThat(map, hasKey(key));
        assertThat(map.get(key), instanceOf(Map.class));
        return (Map<String, Object>) map.get(key);
    }

    @Test
    public void testThatTemplateIsNotOverWritten() throws Exception {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            client().admin().indices().preparePutTemplate("metrics_template").setTemplate("foo*").setSettings("{ \"index.number_of_shards\" : \"1\"}").execute().actionGet(TIMEOUT);

            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates("metrics_template").get(TIMEOUT);

            assertThat(response.getIndexTemplates(), hasSize(1));
            IndexTemplateMetaData templateData = response.getIndexTemplates().get(0);
            assertThat(templateData.template(), is("foo*"));
        }
    }


    @Test
    public void testThatTimeBasedIndicesCanBeDisabled() throws Exception {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().indexDateFormat("").build()) {
            indexWithDate = index;

            registry.counter(name("test", "cache-evictions")).inc();
            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(index).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(1L));
        }
    }

    @Test
    public void testCounter() throws InterruptedException, IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            Counter evictions = registry.counter(name("test", "cache-evictions"));
            evictions.inc(25);
            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(1L));

            Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
            assertTimestamp(hit);
            assertKey(hit, "count", 25);
            assertKey(hit, "name", prefix + ".test.cache-evictions");
            assertKey(hit, "host", "localhost");
        }
    }

    @Test
    public void testHistogram() throws InterruptedException, IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            Histogram histogram = registry.histogram(name("foo", "bar"));
            histogram.update(20);
            histogram.update(40);
            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(1L));

            Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
            assertTimestamp(hit);
            assertKey(hit, "name", prefix + ".foo.bar");
            assertKey(hit, "count", 2);
            assertKey(hit, "max", 40);
            assertKey(hit, "min", 20);
            assertKey(hit, "mean", 30.0);
            assertKey(hit, "host", "localhost");
        }
    }

    @Test
    public void testMeter() throws InterruptedException, IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            Meter meter = registry.meter(name("foo", "bar"));
            meter.mark(10);
            meter.mark(20);
            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(1L));

            Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
            assertTimestamp(hit);
            assertKey(hit, "name", prefix + ".foo.bar");
            assertKey(hit, "count", 30);
            assertKey(hit, "host", "localhost");
        }
    }

    @Test
    public void testTimer() throws Exception {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            Timer timer = registry.timer(name("foo", "bar"));
            Timer.Context timerContext = timer.time();
            Thread.sleep(200);
            timerContext.stop();
            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(1L));

            Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
            assertTimestamp(hit);
            assertKey(hit, "name", prefix + ".foo.bar");
            assertKey(hit, "count", 1);
            assertKey(hit, "host", "localhost");
        }
    }

    @Test
    public void testGauge() throws InterruptedException, IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            registry.register(name("foo", "bar"), (Gauge<Integer>) () -> 1234);
            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(1L));

            Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
            assertTimestamp(hit);
            assertKey(hit, "name", prefix + ".foo.bar");
            assertKey(hit, "value", 1234);
            assertKey(hit, "host", "localhost");
        }
    }

    @Test
    public void nullGaugeShouldBeReported() throws InterruptedException, IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            registry.register(name("foo", "bar"), (Gauge<Integer>) () -> null);
            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(1L));

            Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
            assertTimestamp(hit);
            assertKey(hit, "name", prefix + ".foo.bar");
            assertNullKey(hit, "value");
            assertKey(hit, "host", "localhost");
        }
    }

    @Test
    public void failingGaugeShouldIndexTheError() throws InterruptedException, IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            registry.register(name("foo", "failing"), (Gauge<Integer>) () -> {
                throw new RuntimeException("oups");
            });

            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(1L));

            Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
            assertTimestamp(hit);
            assertKey(hit, "name", prefix + ".foo.failing");
            assertKey(hit, "error", "java.lang.RuntimeException: oups");
            assertKey(hit, "host", "localhost");
        }
    }

    @Test
    public void testThatSpecifyingSeveralHostsWork() throws Exception {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().hosts("localhost:10000", "localhost:" + getPortOfRunningNode()).build()) {

            registry.counter(name("test", "cache-evictions")).inc();
            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(1L));
        }
    }

    @Test
    public void testGracefulFailureIfNoHostIsReachable() throws IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().hosts("localhost:10000").build()) {
            // if no exception is thrown during the test, we consider it all graceful, as we connected to a dead host
            registry.counter(name("test", "cache-evictions")).inc();
            elasticsearchReporter.report();
        }
    }

    @Test
    public void testThatBulkIndexingWorks() throws InterruptedException, IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            for (int i = 0; i < 2020; i++) {
                Counter evictions = registry.counter(name("foo", "bar", String.valueOf(i)));
                evictions.inc(i);
            }
            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(2020L));
        }
    }

    @Test
    public void testThatPercolationNotificationWorks() throws IOException, InterruptedException {
        SimpleNotifier notifier = new SimpleNotifier();
        MetricFilter percolationFilter = (name, metric) -> name.startsWith(prefix + ".foo");

        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder()
            .percolationFilter(percolationFilter)
            .percolationNotifier(notifier)
            .build()) {

            Counter evictions = registry.counter("foo");
            evictions.inc(18);
            reportAndRefresh(elasticsearchReporter);

            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchAllQuery())
                .filter(
                    QueryBuilders.boolQuery()
                        .must(QueryBuilders.rangeQuery("count").gte(20))
                        .must(QueryBuilders.termQuery("name", prefix + ".foo"))
                );
            String json = String.format("{ \"query\" : %s }", queryBuilder.buildAsBytes().toUtf8());
            try {
                client().prepareIndex(indexWithDate, ".percolator", "myName").setRefresh(true).setSource(json).execute().actionGet(TIMEOUT);
            } catch (ElasticsearchTimeoutException e) {
                //ignore, there is a lot of time where the document is inserted but the futur never complete
            }
            evictions.inc(1);
            reportAndRefresh(elasticsearchReporter);
            assertThat(notifier.metrics.size(), is(0));

            evictions.inc(2);
            reportAndRefresh(elasticsearchReporter);
            assertThat(notifier.metrics.size(), is(1));
            assertThat(notifier.metrics, hasKey("myName"));
            assertThat(notifier.metrics.get("myName").name(), is(prefix + ".foo"));

            notifier.metrics.clear();
            evictions.dec(2);
            reportAndRefresh(elasticsearchReporter);
            assertThat(notifier.metrics.size(), is(0));
        }
    }

    @Test
    public void testThatWronglyConfiguredHostDoesNotLeadToApplicationStop() throws IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().hosts("dafuq/1234").build()) {
            elasticsearchReporter.report();
        }
    }

    @Test
    public void testThatTimestampFieldnameCanBeConfigured() throws Exception {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().timestampFieldname("myTimeStampField").build()) {
            registry.counter(name("myMetrics", "cache-evictions")).inc();
            reportAndRefresh(elasticsearchReporter);

            SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("metrics").execute().actionGet(TIMEOUT);
            assertThat(searchResponse.getHits().totalHits(), is(1L));

            Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
            assertThat(hit, hasKey("myTimeStampField"));
        }
    }

    @Test // issue #6
    public void testThatEmptyMetricsDoNotResultInBrokenBulkRequest() throws IOException {
        try (ElasticsearchReporter elasticsearchReporter = createElasticsearchReporterBuilder().build()) {
            long connectionsBeforeReporting = getTotalHttpConnections();
            elasticsearchReporter.report();
            long connectionsAfterReporting = getTotalHttpConnections();

            assertThat(connectionsAfterReporting, is(connectionsBeforeReporting));
        }
    }

    private long getTotalHttpConnections() {
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().setHttp(true).get(TIMEOUT);
        int totalOpenConnections = 0;
        for (NodeStats stats : nodeStats.getNodes()) {
            totalOpenConnections += stats.getHttp().getTotalOpen();
        }
        return totalOpenConnections;
    }

    private class SimpleNotifier implements Notifier {

        public Map<String, JsonMetrics.JsonMetric<? extends Metric>> metrics = new HashMap<>();

        @Override
        public void notify(JsonMetrics.JsonMetric<? extends Metric> jsonMetric, String match) {
            metrics.put(match, jsonMetric);
        }
    }

    private void reportAndRefresh(ElasticsearchReporter elasticsearchReporter) throws InterruptedException {
        elasticsearchReporter.report();
        Thread.sleep(500);
        client().admin().indices().prepareRefresh(indexWithDate).execute().actionGet(TIMEOUT);
    }

    private void assertKey(Map<String, Object> hit, String key, double value) {
        assertKey(hit, key, Double.toString(value));
    }

    private void assertKey(Map<String, Object> hit, String key, int value) {
        assertKey(hit, key, Integer.toString(value));
    }

    private void assertKey(Map<String, Object> hit, String key, String value) {
        assertThat(hit, hasKey(key));
        assertThat(hit.get(key).toString(), is(value));
    }

    private void assertNullKey(Map<String, Object> hit, String key) {
        assertThat(hit, hasKey(key));
        assertThat(hit.get(key), nullValue());
    }

    private void assertTimestamp(Map<String, Object> hit) {
        assertThat(hit, hasKey("@timestamp"));
        // no exception means everything is cool
        ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(hit.get("@timestamp").toString());
    }

    private int getPortOfRunningNode() {
        TransportAddress transportAddress = internalCluster().getInstance(HttpServerTransport.class).boundAddress().publishAddress();
        if (transportAddress instanceof InetSocketTransportAddress) {
            return ((InetSocketTransportAddress) transportAddress).address().getPort();
        }
        throw new ElasticsearchException("Could not find running tcp port");
    }

    private ElasticsearchReporter.Builder createElasticsearchReporterBuilder() {
        Map<String, Object> additionalFields = new HashMap<>();
        additionalFields.put("host", "localhost");
        return ElasticsearchReporter.forRegistry(registry)
            .hosts("localhost:" + getPortOfRunningNode())
            .prefixedWith(prefix)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .filter(MetricFilter.ALL)
            .index(index)
            .additionalFields(additionalFields);
    }
}
