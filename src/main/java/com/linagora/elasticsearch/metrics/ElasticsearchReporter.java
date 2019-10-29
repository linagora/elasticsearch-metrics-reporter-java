/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
package com.linagora.elasticsearch.metrics;

import static com.codahale.metrics.MetricRegistry.name;
import static com.linagora.elasticsearch.metrics.JsonMetrics.JsonCounter;
import static com.linagora.elasticsearch.metrics.JsonMetrics.JsonGauge;
import static com.linagora.elasticsearch.metrics.JsonMetrics.JsonHistogram;
import static com.linagora.elasticsearch.metrics.JsonMetrics.JsonMeter;
import static com.linagora.elasticsearch.metrics.JsonMetrics.JsonMetric;
import static com.linagora.elasticsearch.metrics.JsonMetrics.JsonTimer;
import static com.linagora.elasticsearch.metrics.MetricsElasticsearchModule.BulkIndexOperationHeader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.config.SocketConfig;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultHttpRequestFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.linagora.elasticsearch.metrics.percolation.Notifier;

public class ElasticsearchReporter extends ScheduledReporter {

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private String[] hosts = new String[]{"localhost:9200"};
        private String index = "metrics";
        private String indexDateFormat = "yyyy-MM";
        private int bulkSize = 2500;
        private Notifier percolationNotifier;
        private MetricFilter percolationFilter;
        private int timeout = 5000;
        private String timestampFieldname = "@timestamp";
        private Map<String, ?> additionalFields;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Inject your custom definition of how time passes. Usually the default clock is sufficient
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Configure a prefix for each metric name. Optional, but useful to identify single hosts
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert all the rates to a certain timeunit, defaults to seconds
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert all the durations to a certain timeunit, defaults to milliseconds
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Allows to configure a special MetricFilter, which defines what metrics are reported
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Configure an array of hosts to send data to.
         * Note: Data is always sent to only one host, but this makes sure, that even if a part of your elasticsearch cluster
         * is not running, reporting still happens
         * A host must be in the format hostname:port
         * The port must be the HTTP port of your elasticsearch instance
         */
        public Builder hosts(String... hosts) {
            this.hosts = hosts;
            return this;
        }

        /**
         * The timeout to wait for until a connection attempt is and the next host is tried
         */
        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * The index name to index in
         */
        public Builder index(String index) {
            this.index = index;
            return this;
        }

        /**
         * The index date format used for rolling indices
         * This is appended to the index name, split by a '-'
         */
        public Builder indexDateFormat(String indexDateFormat) {
            this.indexDateFormat = indexDateFormat;
            return this;
        }

        /**
         * The bulk size per request, defaults to 2500 (as metrics are quite small)
         */
        public Builder bulkSize(int bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        /**
         * A metrics filter to define the metrics which should be used for percolation/notification
         */
        public Builder percolationFilter(MetricFilter percolationFilter) {
            this.percolationFilter = percolationFilter;
            return this;
        }

        /**
         * An instance of the notifier implemention which should be executed in case of a matching percolation
         */
        public Builder percolationNotifier(Notifier notifier) {
            this.percolationNotifier = notifier;
            return this;
        }

        /**
         * Configure the name of the timestamp field, defaults to '@timestamp'
         */
        public Builder timestampFieldname(String fieldName) {
            this.timestampFieldname = fieldName;
            return this;
        }

        /**
         * Additional fields to be included for each metric
         *
         * @param additionalFields
         * @return
         */
        public Builder additionalFields(Map<String, ?> additionalFields) {
            this.additionalFields = additionalFields;
            return this;
        }

        public ElasticsearchReporter build() throws IOException {
            return new ElasticsearchReporter(registry,
                hosts,
                timeout,
                index,
                indexDateFormat,
                bulkSize,
                clock,
                prefix,
                rateUnit,
                durationUnit,
                filter,
                percolationFilter,
                percolationNotifier,
                timestampFieldname,
                additionalFields);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchReporter.class);

    private final String[] hosts;
    private final Clock clock;
    private final String prefix;
    private final String index;
    private final int bulkSize;
    private final int timeout;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectWriter writer;
    private MetricFilter percolationFilter;
    private Notifier notifier;
    private String currentIndexName;
    private SimpleDateFormat indexDateFormat = null;
    private boolean checkedForIndexTemplate = false;
    private DefaultHttpRequestFactory httpRequestFactory;
    private CloseableHttpClient httpClient;

    public ElasticsearchReporter(MetricRegistry registry, String[] hosts, int timeout,
                                 String index, String indexDateFormat, int bulkSize, Clock clock, String prefix, TimeUnit rateUnit, TimeUnit durationUnit,
                                 MetricFilter filter, MetricFilter percolationFilter, Notifier percolationNotifier, String timestampFieldname, Map<String, ?> additionalFields) throws MalformedURLException {
        super(registry, "elasticsearch-reporter", filter, rateUnit, durationUnit);
        this.hosts = hosts;
        this.index = index;
        this.bulkSize = bulkSize;
        this.clock = clock;
        this.prefix = prefix;
        this.timeout = timeout;
        if (indexDateFormat != null && indexDateFormat.length() > 0) {
            this.indexDateFormat = new SimpleDateFormat(indexDateFormat);
        }
        if (percolationNotifier != null && percolationFilter != null) {
            this.percolationFilter = percolationFilter;
            this.notifier = percolationNotifier;
        }
        if (timestampFieldname == null || timestampFieldname.trim().length() == 0) {
            LOGGER.error("Timestampfieldname {} is not valid, using default @timestamp", timestampFieldname);
            timestampFieldname = "@timestamp";
        }

        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
        // auto closing means, that the objectmapper is closing after the first write call, which does not work for bulk requests
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        objectMapper.registerModule(new AfterburnerModule());
        objectMapper.registerModule(new MetricsElasticsearchModule(rateUnit, durationUnit, timestampFieldname, additionalFields));
        writer = objectMapper.writer();
        httpRequestFactory = new DefaultHttpRequestFactory();
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(timeout)
            .setSocketTimeout(timeout)
            .setConnectionRequestTimeout(timeout)
            .build();
        SocketConfig socketConfig = SocketConfig.custom()
            .setSoTimeout(timeout)
            .build();
        httpClient = HttpClients.custom()
            .setDefaultRequestConfig(requestConfig)
            .setDefaultSocketConfig(socketConfig)
            .build();
        checkForIndexTemplate();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        // nothing to do if we dont have any metrics to report
        if (gauges.isEmpty() && counters.isEmpty() && histograms.isEmpty() && meters.isEmpty() && timers.isEmpty()) {
            LOGGER.info("All metrics empty, nothing to report");
            return;
        }

        if (!checkedForIndexTemplate) {
            checkForIndexTemplate();
        }
        final long timestamp = clock.getTime() / 1000;

        currentIndexName = index;
        if (indexDateFormat != null) {
            currentIndexName += "-" + indexDateFormat.format(new Date(timestamp * 1000));
        }

        try {
            List<JsonMetric<? extends Metric>> jsonMetrics = ImmutableList.copyOf(Iterables.concat(
                getJsonMetricsForGauges(gauges, timestamp),
                getJsonMetricsForCounters(counters, timestamp),
                getJsonMetricsForHistograms(histograms, timestamp),
                getJsonMetricsForMeters(meters, timestamp),
                getJsonMetricsForTimers(timers, timestamp)
            ));

            List<JsonMetric<? extends Metric>> percolationMetrics = jsonMetrics
                .stream()
                .filter(this::matchPercolationMetric)
                .collect(Collectors.toList());

            for (List<JsonMetric<? extends Metric>> metrics : Iterables.partition(jsonMetrics, bulkSize)) {
                doReportForBatch(metrics);
            }

            // execute the notifier impl, in case percolation found matches
            if (percolationMetrics.size() > 0 && notifier != null) {
                for (JsonMetric jsonMetric : percolationMetrics) {
                    List<String> matches = getPercolationMatches(jsonMetric);
                    for (String match : matches) {
                        notifier.notify(jsonMetric, match);
                    }
                }
            }

            // catch the exception to make sure we do not interrupt the live application
        } catch (IOException e) {
            LOGGER.error("Couldnt report to elasticsearch server", e);
        } catch (FailedtoConnectToElasticSearchException e) {
            LOGGER.error("Could not connect to any configured elasticsearch instances: {}", Arrays.asList(hosts));
        }
    }

    private List<JsonMetric<? extends Metric>> getJsonMetricsForGauges(SortedMap<String, Gauge> gauges, long timestamp) {
        return gauges
            .entrySet()
            .stream()
            .map(entry -> new JsonGauge(name(prefix, entry.getKey()), timestamp, entry.getValue()))
            .collect(Collectors.toList());
    }

    private List<JsonMetric<? extends Metric>> getJsonMetricsForCounters(SortedMap<String, Counter> counters, long timestamp) {
        return counters
            .entrySet()
            .stream()
            .map(entry -> new JsonCounter(name(prefix, entry.getKey()), timestamp, entry.getValue()))
            .collect(Collectors.toList());
    }

    private List<JsonMetric<? extends Metric>> getJsonMetricsForHistograms(SortedMap<String, Histogram> histograms, long timestamp) {
        return histograms
            .entrySet()
            .stream()
            .map(entry -> new JsonHistogram(name(prefix, entry.getKey()), timestamp, entry.getValue()))
            .collect(Collectors.toList());
    }

    private List<JsonMetric<? extends Metric>> getJsonMetricsForMeters(SortedMap<String, Meter> meters, long timestamp) {
        return meters
            .entrySet()
            .stream()
            .map(entry -> new JsonMeter(name(prefix, entry.getKey()), timestamp, entry.getValue()))
            .collect(Collectors.toList());
    }

    private List<JsonMetric<? extends Metric>> getJsonMetricsForTimers(SortedMap<String, Timer> timers, long timestamp) {
        return timers.entrySet()
            .stream()
            .map(entry -> new JsonTimer(name(prefix, entry.getKey()), timestamp, entry.getValue()))
            .collect(Collectors.toList());
    }

    private void doReportForBatch(List<JsonMetric<? extends Metric>> jsonMetrics) throws FailedtoConnectToElasticSearchException, IOException {
        HttpPost request = new HttpPost("/_bulk");
        request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        request.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        for (JsonMetric jsonMetric : jsonMetrics) {
            writeJsonMetric(jsonMetric, writer, outputStream);
        }

        request.setEntity(new ByteArrayEntity(outputStream.toByteArray()));
        try (CloseableHttpResponse response = executeRequest(request)) {
            if (response.getStatusLine().getStatusCode() != 200) {
                LOGGER.error("Reporting returned code {} : {}", response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
            }
        }
    }

    /**
     * Execute a percolation request for the specified metric
     */
    private List<String> getPercolationMatches(JsonMetric<? extends Metric> jsonMetric) throws IOException {

        HttpPost request = new HttpPost("/" + currentIndexName + "/" + jsonMetric.type() + "/_percolate");
        request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        request.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Map<String, Object> data = new HashMap<>(1);
        data.put("doc", jsonMetric);
        objectMapper.writeValue(outputStream, data);

        request.setEntity(new ByteArrayEntity(outputStream.toByteArray()));

        try (CloseableHttpResponse response = executeRequest(request)) {
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("Error percolating " + jsonMetric);
            }
            Map<String, Object> input = objectMapper.readValue(response.getEntity().getContent(), new TypeReference<Map<String, Object>>() {});
            List<String> matches = new ArrayList<>();
            if (input.containsKey("matches") && input.get("matches") instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, String>> foundMatches = (List<Map<String, String>>) input.get("matches");
                for (Map<String, String> entry : foundMatches) {
                    if (entry.containsKey("_id")) {
                        matches.add(entry.get("_id"));
                    }
                }
            }
            return matches;
        } catch (FailedtoConnectToElasticSearchException e) {
            LOGGER.error("Could not connect to any configured elasticsearch instances for percolation: {}", Arrays.asList(hosts));
            return Collections.emptyList();
        }
    }

    private boolean matchPercolationMetric(JsonMetric<? extends Metric> jsonMetric) {
        return (percolationFilter != null && percolationFilter.matches(jsonMetric.name(), jsonMetric.value()));
    }

    /**
     * serialize a JSON metric over the outputstream in a bulk request
     */
    private void writeJsonMetric(JsonMetric<? extends Metric> jsonMetric, ObjectWriter writer, OutputStream out) throws IOException {
        writer.writeValue(out, new BulkIndexOperationHeader(currentIndexName, jsonMetric.type()));
        out.write("\n".getBytes());
        writer.writeValue(out, jsonMetric);
        out.write("\n".getBytes());

        out.flush();
    }

    /**
     * Try to execute the request, in case it fails it tries for the next host in the configured list
     */
    private CloseableHttpResponse executeRequest(String method, String uri) throws FailedtoConnectToElasticSearchException {
        for (String host : hosts) {
            try {
                HttpHost httpHost = HttpHost.create("http://" + host);
                HttpRequest request = httpRequestFactory.newHttpRequest(method, uri);
                request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
                request.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");

                return httpClient.execute(httpHost, request);
            } catch (Exception e) {
                LOGGER.error("Error connecting to {}: {}", host, e);
                if (hosts[hosts.length - 1].equals(host)) {
                    LOGGER.error("Could not connect to any configured elasticsearch instances: {} for request {}", Arrays.asList(hosts), uri);
                    throw new FailedtoConnectToElasticSearchException();
                }
            }
        }
        throw new FailedtoConnectToElasticSearchException();
    }

    /**
     * Try to execute the request, in case it fails it tries for the next host in the configured list
     */
    private CloseableHttpResponse executeRequest(HttpRequest request) throws FailedtoConnectToElasticSearchException {
        for (String host : hosts) {
            try {
                HttpHost httpHost = HttpHost.create("http://" + host);
                return httpClient.execute(httpHost, request);
            } catch (Exception e) {
                LOGGER.error("Error connecting to {}: {}", host, e);
                if (hosts[hosts.length - 1].equals(host)) {
                    LOGGER.error("Could not connect to any configured elasticsearch instances: {} for request {}", Arrays.asList(hosts), request.getRequestLine().getUri());
                    throw new FailedtoConnectToElasticSearchException();
                }
            }
        }
        throw new FailedtoConnectToElasticSearchException();
    }

    private boolean isTemplateMissing() throws FailedtoConnectToElasticSearchException, IOException {
        try (CloseableHttpResponse response = executeRequest("HEAD", "/_template/metrics_template")) {
            return response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND;
        }
    }

    /**
     * This index template is automatically applied to all indices which start with the index name
     * The index template simply configures the name not to be analyzed
     */
    private void checkForIndexTemplate() {
        try {
            if (isTemplateMissing()) {
                LOGGER.debug("No metrics template found in elasticsearch. Adding...");
                HttpPut putTemplateRequest = new HttpPut("/_template/metrics_template");
                putTemplateRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
                putTemplateRequest.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
                byte[] requestJsonBody = writeIndexTemplateJson();
                putTemplateRequest.setEntity(new ByteArrayEntity(requestJsonBody));

                try (CloseableHttpResponse putTemplateResponse = executeRequest(putTemplateRequest)) {
                    if (putTemplateResponse.getStatusLine().getStatusCode() != 200) {
                        LOGGER.error("Error adding metrics template to elasticsearch: {}/{}",
                            putTemplateResponse.getStatusLine().getStatusCode(), putTemplateResponse.getStatusLine().getReasonPhrase());
                    }
                    checkedForIndexTemplate = true;
                } catch (FailedtoConnectToElasticSearchException e) {
                    LOGGER.error("Error adding metrics template to elasticsearch", e);
                }
            } else {
                checkedForIndexTemplate = true;
            }
        } catch (IOException | FailedtoConnectToElasticSearchException e) {
            LOGGER.error("Error when checking/adding metrics template to elasticsearch", e);
        }
    }

    private byte[] writeIndexTemplateJson() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        JsonGenerator json = new JsonFactory().createGenerator(outputStream);
        json.writeStartObject();
        json.writeStringField("template", index + "*");
        json.writeObjectFieldStart("mappings");

        json.writeObjectFieldStart("_default_");
        json.writeObjectFieldStart("_all");
        json.writeBooleanField("enabled", false);
        json.writeEndObject();
        json.writeObjectFieldStart("properties");
        json.writeObjectFieldStart("name");
        json.writeObjectField("type", "string");
        json.writeObjectField("index", "not_analyzed");
        json.writeEndObject();
        json.writeEndObject();
        json.writeEndObject();

        json.writeEndObject();
        json.writeEndObject();
        json.flush();

        return outputStream.toByteArray();
    }

    @Override
    public void close() {
        super.close();
        try {
            httpClient.close();
        } catch (IOException e) {
            LOGGER.error("Error when closing the http client", e);
        }
    }
}
