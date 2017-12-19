package ch.cern.spark.metrics.source.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Test;

import ch.cern.spark.metrics.Metric;

public class KafkaMetricsSourceTest extends MetricsStreamFromKafkaProvider{

	private static final long serialVersionUID = 5012067774243443422L;

	@Test
	public void parse() throws Exception {
		List<Metric> inputMetrics = new LinkedList<Metric>();
		Map<String, String> ids = new HashMap<>();
		ids.put("$source", "kafka");
		ids.put("$schema", "kafka");
		ids.put("$value_attribute", "VALUE");
		inputMetrics.add(new Metric(Instant.now(), (float) Math.random(), ids));
		inputMetrics.add(new Metric(Instant.now(), (float) Math.random(), ids));
		inputMetrics.add(new Metric(Instant.now(), (float) Math.random(), ids));
		
		ids.put("KEY_TO_REMOVE", "something");
		inputMetrics.add(new Metric(Instant.now(), (float) Math.random(), ids));
		
		JavaDStream<Metric> metrics = createStream();

		List<Metric> outputMetrics = new LinkedList<>();
		metrics.foreachRDD(rdd -> {
			List<Metric> metricsList = rdd.collect();
			outputMetrics.addAll(metricsList);
		});
		
		start(1);
		
		sendMetrics(inputMetrics);
		
		assertTrue(waitUntilBatchesCompleted(1));
		
		inputMetrics.removeIf(m -> m.getIDs().containsKey("KEY_TO_REMOVE"));
		assertEquals(inputMetrics, outputMetrics);
	}

}