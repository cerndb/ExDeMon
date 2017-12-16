package ch.cern.spark.metrics.defined;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.Properties;
import ch.cern.spark.Stream;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.metrics.Metric;

public class DefinedMetricsTest extends StreamTestHelper<Metric, Metric> {

	private static final long serialVersionUID = -2224810535022352025L;
	
	@Before
	public void reset() throws Exception {
		super.setUp();
		Properties.initCache(null);
		DefinedMetrics.getCache().reset();
	}

	@Test
	public void shouldUpdateDefinedMetrics() throws Exception {
		DefinedMetrics.initCache(null);
		
		Cache<Map<String, DefinedMetric>> definedMetricsCache = DefinedMetrics.getCache();
				
		Properties.getCache().reset();
		definedMetricsCache.setExpiration(Duration.ofSeconds(1));
		
		//First load
		Map<String, DefinedMetric> original = definedMetricsCache.get();
		
		Map<String, DefinedMetric> returned = definedMetricsCache.get();
		assertSame(original, returned);
		
		Thread.sleep(Duration.ofMillis(100).toMillis());
		
		returned = definedMetricsCache.get();
		assertSame(original, returned);
		
		Thread.sleep(Duration.ofSeconds(1).toMillis());
		
		returned = definedMetricsCache.get();
		assertNotSame(original, returned);
	}

	@Test
	public void shouldGenerateMetrics() throws Exception {
		DefinedMetrics.getCache().reset();
		
        addInput(0,    Metric(1, 10f, "HOSTNAME=host1"));
        addInput(0,    Metric(1, 20f, "HOSTNAME=host2"));
        addExpected(0, Metric(1, 30f, "$defined_metric=dm1"));
        
        addInput(1,    Metric(2, 20f, "HOSTNAME=host1"));
        addInput(1,    Metric(2, 30f, "HOSTNAME=host2"));
        addExpected(1, Metric(2, 50f, "$defined_metric=dm1"));
		
        Cache<Properties> propertiesCache = Properties.getCache();
        propertiesCache.set(new Properties());
        propertiesCache.get().setProperty("metrics.define.dm1.variables.a.filter.attribute.HOSTNAME", ".*");
        propertiesCache.get().setProperty("metrics.define.dm1.variables.a.aggregate", "sum");
        propertiesCache.get().setProperty("metrics.define.dm1.when", "batch");
	        
        Stream<Metric> metricsStream = createStream(Metric.class);
        
		Stream<Metric> results = DefinedMetrics.generate(metricsStream, null, Optional.empty());
        
        assertExpected(results);
	}
	
	@Test
	public void shouldApplyDefinedMetricFilter() throws Exception {
		DefinedMetrics.getCache().reset();
		
        addInput(0,    Metric(1, 10f, "CLUSTER=c1", "HOSTNAME=host1"));
        addInput(0,    Metric(1, 20f, "CLUSTER=c1", "HOSTNAME=host2"));
        addExpected(0, Metric(1, 30f, "$defined_metric=dm1"));
        
        addInput(1,    Metric(2, 7f,  "CLUSTER=c22", "HOSTNAME=host1"));
        addInput(1,    Metric(2, 13f, "CLUSTER=c22", "HOSTNAME=host2"));
        addExpected(1, Metric(2, 30f, "$defined_metric=dm1"));
        
        addInput(2,    Metric(3, 20f, "CLUSTER=c1", "HOSTNAME=host1"));
        addInput(2,    Metric(3, 30f, "CLUSTER=c1", "HOSTNAME=host2"));
        addExpected(2, Metric(3, 50f, "$defined_metric=dm1"));
		
        Cache<Properties> propertiesCache = Properties.getCache();
        propertiesCache.set(new Properties());
        propertiesCache.get().setProperty("metrics.define.dm1.metrics.filter.attribute.CLUSTER", "c1");
        propertiesCache.get().setProperty("metrics.define.dm1.variables.a.filter.attribute.HOSTNAME", ".*");
        propertiesCache.get().setProperty("metrics.define.dm1.variables.a.aggregate", "sum");
        propertiesCache.get().setProperty("metrics.define.dm1.when", "batch");
	        
        Stream<Metric> metricsStream = createStream(Metric.class);
        
		Stream<Metric> results = DefinedMetrics.generate(metricsStream, null, Optional.empty());
        
        assertExpected(results);
	}
    
	@Test
	public void shouldGenerateMetricsWithComplexEquation() throws Exception {
		DefinedMetrics.getCache().reset();
		
        addInput(0,    Metric(1, true, 		"TYPE=DirReport", "$value_attribute=monitor_enable"));
        addInput(0,    Metric(1, " /tmp/  ", "TYPE=DirReport", "$value_attribute=path"));
        addInput(0,    Metric(1, 100, 		"TYPE=DirReport", "$value_attribute=used_bytes"));
        addInput(0,    Metric(1, 1000, 		"TYPE=DirReport", "$value_attribute=capacity_bytes"));
        // !true || ((trim("/tmp/") == "/tmp/") && (abs(100 / 1000) > 0.8))
        // false || ((true) && (0.1 > 0.8))
        	// false || ((true) && (false))
        // false || (false)
        // false
        addExpected(0, Metric(1, false, 		"$defined_metric=dm1"));
        
        addInput(1,    Metric(2, true, 		"TYPE=DirReport", "$value_attribute=monitor_enable"));
        addInput(1,    Metric(2, " /tmp/ ",  "TYPE=DirReport", "$value_attribute=path"));
        addInput(1,    Metric(2, 900, 		"TYPE=DirReport", "$value_attribute=used_bytes"));
        addInput(1,    Metric(2, 1000, 		"TYPE=DirReport", "$value_attribute=capacity_bytes"));
        // !true || ((trim("/tmp/") == "/tmp/") && (abs(900 / 1000) > 0.8))
        // false || ((true) && (0.9 > 0.8))
        	// false || ((true) && (true))
        // false || (true)
        // true
        addExpected(1, Metric(2, true, 		"$defined_metric=dm1"));
		
        Cache<Properties> propertiesCache = Properties.getCache();
        propertiesCache.set(new Properties());
        propertiesCache.get().setProperty("metrics.define.dm1.metrics.filter.attribute.TYPE", "DirReport");
        propertiesCache.get().setProperty("metrics.define.dm1.value", "!shouldBeMonitored || ((trim(dir) == \"/tmp/\") && (abs(used / capacity) > 0.8))");
        propertiesCache.get().setProperty("metrics.define.dm1.variables.shouldBeMonitored.filter.attribute.$value_attribute", "monitor_enable");
        propertiesCache.get().setProperty("metrics.define.dm1.variables.dir.filter.attribute.$value_attribute", "path");
        propertiesCache.get().setProperty("metrics.define.dm1.variables.used.filter.attribute.$value_attribute", "used_bytes");
        propertiesCache.get().setProperty("metrics.define.dm1.variables.capacity.filter.attribute.$value_attribute", "capacity_bytes");
        propertiesCache.get().setProperty("metrics.define.dm1.when", "batch");
	        
        Stream<Metric> metricsStream = createStream(Metric.class);
        
		Stream<Metric> results = DefinedMetrics.generate(metricsStream, null, Optional.empty());
        
        assertExpected(results);
	}
	
}
