package ch.cern.spark.metrics.monitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.MetricTest;
import ch.cern.spark.metrics.defined.DefinedMetrics;
import scala.Tuple2;

public class ComputeMonitorKeysFTest {
	
	@Before
	public void reset() throws ConfigurationException {
		Properties.initCache(null);
		Monitors.getCache().reset();
		DefinedMetrics.getCache().reset();
	}

    @Test
    public void oneMonitor() throws Exception{
    		Cache<Properties> propertiesCache = Properties.getCache();
    		Properties properties = new Properties();
    		properties.setProperty("monitor.ID-1.analysis.type", "true");
    		properties.setProperty("monitor.ID-1.filter.attribute.key1", "val1");
    		properties.setProperty("monitor.ID-1.filter.attribute.key2", "val2");
    		propertiesCache.set(properties);
    		
        Metric metric = MetricTest.build();
        
        Iterator<Tuple2<MonitorStatusKey, Metric>> result = new ComputeMonitorKeysF(null).call(metric);
        
        assertResult(result, metric, "ID-1");
        
        assertFalse(result.hasNext());
    }

    @Test
    public void severalMonitors() throws Exception{
    		Cache<Properties> propertiesCache = Properties.getCache();
    		Properties properties = new Properties();
    		properties.setProperty("monitor.ID-1.analysis.type", "true");
    		properties.setProperty("monitor.ID-1.filter.attribute.key1", "val1");
    		properties.setProperty("monitor.ID-1.filter.attribute.key2", "val2");
    		properties.setProperty("monitor.ID-2.analysis.type", "true");
    		properties.setProperty("monitor.ID-2.filter.attribute.key1", "val1");
    		properties.setProperty("monitor.ID-3.analysis.type", "true");
    		properties.setProperty("monitor.ID-3.filter.attribute.key2", "val2");
    		properties.setProperty("monitor.ID-3.filter.attribute.key3", "val3");
    		properties.setProperty("monitor.ID-4.analysis.type", "true");
    		properties.setProperty("monitor.ID-4.filter.attribute.key3", "NO");
    		propertiesCache.set(properties);
    		
        Metric metric = MetricTest.build();
        
        Iterator<Tuple2<MonitorStatusKey, Metric>> result = new ComputeMonitorKeysF(null).call(metric);
        
        assertResult(result, metric, "ID-2");
        assertResult(result, metric, "ID-3");
        assertResult(result, metric, "ID-1");
        
        assertFalse(result.hasNext());
    }
    
    private void assertResult(Iterator<Tuple2<MonitorStatusKey, Metric>> result, Metric metric, String id) {
        assertTrue(result.hasNext());
        
        Tuple2<MonitorStatusKey, Metric> tuple = result.next();
        
        assertEquals(id, tuple._1.getID());
        assertSame(metric.getIDs(), tuple._1.getMetricIDs());
        
        assertSame(metric, tuple._2);
    }
    
}
