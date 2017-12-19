package ch.cern.spark.metrics.monitors;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.Properties;
import ch.cern.spark.Batches;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.DefinedMetrics;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.schema.MetricSchemas;

public class MonitorReturningNotificationsStreamTest extends StreamTestHelper<Metric, Notification> {
	
	private static final long serialVersionUID = -444431845152738589L;
	
	@Before
	public void setUp() throws Exception {
		super.setUp();
		
		Properties.initCache(null);
		Monitors.getCache().reset();	
		DefinedMetrics.getCache().reset();
		MetricSchemas.getCache().reset();
	}

	@Test
	public void shouldProducePeriodicNotificationsWithConfigurationExceptionAndFilterOK() throws Exception {
		Cache<Properties> propertiesCache = Properties.getCache();
        Properties properties = new Properties();
        properties.setProperty("monitor.mon1.filter.expr", "HOST=host1");
		properties.setProperty("monitor.mon1.analysis.type", "does not exist");
        propertiesCache.set(properties);
        
		setBatchDuration(5 * 60);
        
        Instant now = Instant.now();
        addInput(0,    Metric(now, 0, "HOST=host1"));
        addInput(0,    Metric(now.plus(Duration.ofMinutes(3)), 0, "HOST=host1"));
        addInput(0,    Metric(now.plus(Duration.ofMinutes(4)), 0, "HOST=host2"));
        
        addInput(1,    Metric(now.plus(Duration.ofMinutes(8)), 0, "HOST=host1"));
        addInput(1,    Metric(now.plus(Duration.ofMinutes(9)), 0, "HOST=host2"));
        
        addInput(2,    Metric(now.plus(Duration.ofMinutes(12)), 0, "HOST=host1"));
        addInput(2,    Metric(now.plus(Duration.ofMinutes(13)), 0));
        
        addInput(3,    Metric(now.plus(Duration.ofMinutes(16)), 0, "HOST=host1"));
        addInput(3,    Metric(now.plus(Duration.ofMinutes(18)), 0, "HOST=host2"));
        addInput(3,    Metric(now.plus(Duration.ofMinutes(19)), 0, "HOST=host2"));
        
        addInput(4,    Metric(now.plus(Duration.ofMinutes(21)), 0, "HOST=hostasdf"));
        addInput(4,    Metric(now.plus(Duration.ofMinutes(22)), 0, "HOST=hostasfd"));
        addInput(4,    Metric(now.plus(Duration.ofMinutes(24)), 0, "HOST=host324"));
        
        addInput(5,    Metric(now.plus(Duration.ofMinutes(26)), 0, "HOST=host1"));
        JavaDStream<Metric> metricsStream = createStream(Metric.class);
        
        JavaDStream<Notification> results = Monitors.notify(Monitors.analyze(metricsStream, null, Optional.empty()), null, Optional.empty());
        
        Batches<Notification> returnedBatches = collect(results);
        
        List<Notification> batch0 = returnedBatches.get(0);
        assertEquals(0, batch0.size());
        
        List<Notification> batch1 = returnedBatches.get(1);
        assertEquals(0, batch1.size());
        
        List<Notification> batch2 = returnedBatches.get(2);
        assertEquals(1, batch2.size());
        assertEquals(1, batch2.get(0).getMetricIDs().size());
        assertEquals("host1", batch2.get(0).getMetricIDs().get("HOST"));
        assertEquals(1, batch2.get(0).getSinkIds().size());
        assertTrue(batch2.get(0).getSinkIds().contains("ALL"));
        
        List<Notification> batch3 = returnedBatches.get(3);
        assertEquals(0, batch3.size());
        
        List<Notification> batch4 = returnedBatches.get(4);
        assertEquals(0, batch4.size());
        
        List<Notification> batch5 = returnedBatches.get(5);
        assertEquals(1, batch5.size());
        assertEquals(1, batch5.get(0).getMetricIDs().size());
        assertEquals("host1", batch5.get(0).getMetricIDs().get("HOST"));
        assertEquals(1, batch2.get(0).getSinkIds().size());
        assertTrue(batch2.get(0).getSinkIds().contains("ALL"));
	}
	
	@Test
	public void shouldProducePeriodicNotificationsWithFilterConfigurationException() throws Exception {
		setBatchDuration(5 * 60);

        Cache<Properties> propertiesCache = Properties.getCache();
        Properties properties = new Properties();
		properties.setProperty("monitor.mon1.filter.expr", "does not work");
		properties.setProperty("monitor.mon1.analysis.type", "true");
		properties.setProperty("monitor.mon1.analysis.error.upperbound", "20");
		properties.setProperty("monitor.mon1.analysis.error.lowerbound", "10");
        propertiesCache.set(properties);
        
        Instant now = Instant.now();
        addInput(0,    Metric(now, 0, "HOST=host1"));
        addInput(0,    Metric(now.plus(Duration.ofMinutes(3)), 0, "HOST=host1"));
        addInput(0,    Metric(now.plus(Duration.ofMinutes(4)), 0, "HOST=host2"));
        
        addInput(1,    Metric(now.plus(Duration.ofMinutes(8)), 0, "HOST=host1"));
        addInput(1,    Metric(now.plus(Duration.ofMinutes(9)), 0, "HOST=host2"));
        
        addInput(2,    Metric(now.plus(Duration.ofMinutes(12)), 0, "HOST=host1"));
        addInput(2,    Metric(now.plus(Duration.ofMinutes(13)), 0));
        
        addInput(3,    Metric(now.plus(Duration.ofMinutes(16)), 0, "HOST=host1"));
        addInput(3,    Metric(now.plus(Duration.ofMinutes(18)), 0, "HOST=host2"));
        addInput(3,    Metric(now.plus(Duration.ofMinutes(19)), 0, "HOST=host2"));
        
        addInput(4,    Metric(now.plus(Duration.ofMinutes(21)), 0, "HOST=hostasdf"));
        addInput(4,    Metric(now.plus(Duration.ofMinutes(22)), 0, "HOST=hostasfd"));
        addInput(4,    Metric(now.plus(Duration.ofMinutes(24)), 0, "HOST=host1"));
        JavaDStream<Metric> metricsStream = createStream(Metric.class);
        
        JavaDStream<Notification> results = Monitors.notify(Monitors.analyze(metricsStream, null, Optional.empty()), null, Optional.empty());
        
        Batches<Notification> returnedBatches = collect(results);
        
        List<Notification> batch0 = returnedBatches.get(0);
        assertEquals(0, batch0.size());
        
        List<Notification> batch1 = returnedBatches.get(1);
        assertEquals(0, batch1.size());
        
        List<Notification> batch2 = returnedBatches.get(2);
        assertEquals(1, batch2.size());
        assertEquals(0, batch2.get(0).getMetricIDs().size());
        
        List<Notification> batch3 = returnedBatches.get(3);
        assertEquals(0, batch3.size());
        
        List<Notification> batch4 = returnedBatches.get(4);
        assertEquals(1, batch4.size());
        assertEquals(0, batch2.get(0).getMetricIDs().size());
	}

}
