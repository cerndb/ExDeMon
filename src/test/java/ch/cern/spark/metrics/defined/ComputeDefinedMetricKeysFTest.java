package ch.cern.spark.metrics.defined;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import scala.Tuple2;

public class ComputeDefinedMetricKeysFTest {
	
	private Cache<Map<String, DefinedMetric>> definedMetricsCache = DefinedMetrics.getCache();
	
	@Before
	public void reset() throws ConfigurationException {
		Properties.initCache(null);
		Properties.getCache().reset();
		definedMetricsCache.reset();
	}

	@Test
	public void oneMetricsIsMultuplyedBySeveralDefinedMetrics() throws Exception {
		ComputeDefinedMetricKeysF function = getTwoDefinedMetricsWithOneGroupByAttribute();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1");
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricStatuskey, Metric>> result;
		
		result = function.call(metric);
		assertTrue(result.hasNext());
		Tuple2<DefinedMetricStatuskey, Metric> tuple = result.next();
		assertEquals("defM1", tuple._1.getID());
		assertEquals(1, tuple._1.getGroupByMetricIDs().size());
		assertEquals("prod-machine1", tuple._1.getGroupByMetricIDs().get("INSTANCE_NAME"));
		assertSame(metric, tuple._2);
		tuple = result.next();
		assertEquals("defM2", tuple._1.getID());
		assertEquals(1, tuple._1.getGroupByMetricIDs().size());
		assertEquals("prod-machine1", tuple._1.getGroupByMetricIDs().get("INSTANCE_NAME"));
		assertSame(metric, tuple._2);
		assertFalse(result.hasNext());
		
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine2");
		ids.put("METRIC_NAME", "Host CPU Usage Per Sec");
		metric = new Metric(null, 0f, ids);
		
		result = function.call(metric);
		assertTrue(result.hasNext());
		tuple = result.next();
		assertEquals("defM1", tuple._1.getID());
		assertEquals(1, tuple._1.getGroupByMetricIDs().size());
		assertEquals("prod-machine2", tuple._1.getGroupByMetricIDs().get("INSTANCE_NAME"));
		assertSame(metric, tuple._2);
		tuple = result.next();
		assertEquals("defM2", tuple._1.getID());
		assertEquals(1, tuple._1.getGroupByMetricIDs().size());
		assertEquals("prod-machine2", tuple._1.getGroupByMetricIDs().get("INSTANCE_NAME"));
		assertSame(metric, tuple._2);
		assertFalse(result.hasNext());
	}
	
	@Test
	public void metricDoNotPassFilters() throws Exception {
		ComputeDefinedMetricKeysF function = getTwoDefinedMetricsWithOneGroupByAttribute();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1");
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricStatuskey, Metric>> result;
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine2");
		ids.put("METRIC_NAME", "To be filtered out");
		metric = new Metric(null, 0f, ids);
		
		result = function.call(metric);
		assertFalse(result.hasNext());
		
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "to be filter out by defM1");
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		metric = new Metric(null, 0f, ids);
		
		result = function.call(metric);
		assertTrue(result.hasNext());
		Tuple2<DefinedMetricStatuskey, Metric> tuple = result.next();
		assertEquals("defM2", tuple._1.getID());
		assertFalse(result.hasNext());
	}
	
	@Test
	public void groupByAll() throws Exception {
		ComputeDefinedMetricKeysF function = getGroupByAll();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1");
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		ids.put("METRIC_NAME_SHORT", "CPUUsage");
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricStatuskey, Metric>> result = function.call(metric);
		assertTrue(result.hasNext());
		Tuple2<DefinedMetricStatuskey, Metric> tuple = result.next();
		assertEquals("defMAll", tuple._1.getID());
		assertEquals(3, tuple._1.getGroupByMetricIDs().size());
		assertEquals("prod-machine1", tuple._1.getGroupByMetricIDs().get("INSTANCE_NAME"));
		assertEquals("CPU Usage Per Sec", tuple._1.getGroupByMetricIDs().get("METRIC_NAME"));
		assertEquals("CPUUsage", tuple._1.getGroupByMetricIDs().get("METRIC_NAME_SHORT"));
		assertFalse(result.hasNext());
	}
	
	@Test
	public void metricMissingGrouByAttributeShouldNotPass() throws Exception {
		ComputeDefinedMetricKeysF function = getTwoDefinedMetricsWithOneGroupByAttribute();
		
		Map<String, String> ids = new HashMap<>();
//		ids.put("INSTANCE_NAME", null); <- INSTANCE_NAME is in groupby
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricStatuskey, Metric>> result = function.call(metric);
		assertFalse(result.hasNext());
	}
	
	@Test
	public void groupByAllDoNotPassAnyFIlter() throws Exception {
		ComputeDefinedMetricKeysF function = getGroupByAll();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "dev-machine1"); //to be filtered out
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		ids.put("METRIC_NAME_SHORT", "CPUUsage"); //is not filter anywhere
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricStatuskey, Metric>> result = function.call(metric);
		assertFalse(result.hasNext());
		
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1"); 
		ids.put("METRIC_NAME", "Not listed metric name"); //to be filtered out
		ids.put("METRIC_NAME_SHORT", "CPUUsage");  //is not filter anywhere
		metric = new Metric(null, 0f, ids);
		
		result = function.call(metric);
		assertFalse(result.hasNext());
	}
	
	@Test
	public void groupByNone() throws Exception {
		ComputeDefinedMetricKeysF function = getGroupByNone();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1");
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		ids.put("METRIC_NAME_SHORT", "CPUUsage");
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricStatuskey, Metric>> result = function.call(metric);
		assertTrue(result.hasNext());
		Tuple2<DefinedMetricStatuskey, Metric> tuple = result.next();
		assertEquals("defMNone", tuple._1.getID());
		assertEquals(0, tuple._1.getGroupByMetricIDs().size());
		assertFalse(result.hasNext());
	}
	
	@Test
	public void groupByNoneDoNotPassAnyFIlter() throws Exception {
		ComputeDefinedMetricKeysF function = getGroupByAll();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "dev-machine1"); //to be filtered out
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		ids.put("METRIC_NAME_SHORT", "CPUUsage"); //is not filter anywhere
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricStatuskey, Metric>> result = function.call(metric);
		assertFalse(result.hasNext());
		
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1"); 
		ids.put("METRIC_NAME", "Not listed metric name"); //to be filtered out
		ids.put("METRIC_NAME_SHORT", "CPUUsage");  //is not filter anywhere
		metric = new Metric(null, 0f, ids);
		
		result = function.call(metric);
		assertFalse(result.hasNext());
	}

	private ComputeDefinedMetricKeysF getTwoDefinedMetricsWithOneGroupByAttribute() throws Exception {
		Map<String, DefinedMetric> definedMetrics = new HashMap<>();
		
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("metrics.groupby", "INSTANCE_NAME");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "prod-.*");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "prod-.*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		DefinedMetric definedMetric1 = new DefinedMetric("defM1").config(properties);
		definedMetrics.put(definedMetric1.getName(), definedMetric1);
		
		properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("metrics.groupby", "INSTANCE_NAME");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		DefinedMetric definedMetric2 = new DefinedMetric("defM2").config(properties);
		definedMetrics.put(definedMetric2.getName(), definedMetric2);

		definedMetricsCache.set(definedMetrics);
		
		return new ComputeDefinedMetricKeysF(null);
	}
	
	private ComputeDefinedMetricKeysF getGroupByAll() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("metrics.groupby", "ALL");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "prod-.*");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "prod-.*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		
		Map<String, DefinedMetric> definedMetrics = new HashMap<>();
		DefinedMetric definedMetric = new DefinedMetric("defMAll").config(properties);
		definedMetrics.put(definedMetric.getName(), definedMetric);
		definedMetricsCache.set(definedMetrics);
		
		return new ComputeDefinedMetricKeysF(null);
	}
	
	private ComputeDefinedMetricKeysF getGroupByNone() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
//		properties.setProperty("metric.groupby", null);
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "prod-.*");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "prod-.*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		
		Map<String, DefinedMetric> definedMetrics = new HashMap<>();
		DefinedMetric definedMetric = new DefinedMetric("defMNone").config(properties);
		definedMetrics.put(definedMetric.getName(), definedMetric);
		definedMetricsCache.set(definedMetrics);
		
		return new ComputeDefinedMetricKeysF(null);
	}
	
}
