package com.linagora.elasticsearch.metrics;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linagora.elasticsearch.metrics.JsonMetrics.JsonGauge;
import com.linagora.elasticsearch.metrics.JsonMetrics.JsonMetric;

/**
 * Tests if value is an array.
 * @author static-max
 *
 */
public class MetricsElasticsearchModuleTest {
	@Test
	public void test() {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.registerModule(new MetricsElasticsearchModule(TimeUnit.MINUTES, TimeUnit.MINUTES, "@timestamp", null));
		
		Gauge<String> gaugeString = () -> "STRING VALUE";
		
		/**
		 * Used for deadlocks in metrics-jvm ThreadStatesGaugeSet.class.
		 */
		Gauge<Set<String>> gaugeStringSet = () -> ImmutableSet.of("1", "2", "3");
		
		
		JsonMetric<?> jsonMetricString = new JsonGauge("string", Long.MAX_VALUE, gaugeString);
		JsonMetric<?> jsonMetricStringSet = new JsonGauge("string", Long.MAX_VALUE, gaugeStringSet);
		
		JsonNode stringNode = objectMapper.valueToTree(jsonMetricString);
		assertTrue(stringNode.get("value").isTextual());
		assertFalse(stringNode.get("value").isNumber());
		
		JsonNode stringSetNode = objectMapper.valueToTree(jsonMetricStringSet);
		assertTrue(stringSetNode.get("value").isArray());
		assertTrue(stringSetNode.get("value").get(0).isTextual());
		assertTrue(stringSetNode.get("value").get(1).isTextual());
		assertTrue(stringSetNode.get("value").get(2).isTextual());
	}

}