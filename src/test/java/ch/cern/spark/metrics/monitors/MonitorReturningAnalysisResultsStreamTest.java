package ch.cern.spark.metrics.monitors;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.Properties;
import ch.cern.spark.Batches;
import ch.cern.spark.Stream;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.DefinedMetrics;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.schema.MetricSchemas;

public class MonitorReturningAnalysisResultsStreamTest extends StreamTestHelper<Metric, AnalysisResult> {
	
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
	public void monitorAndDefinedMetric() throws Exception {
        Cache<Properties> propertiesCache = Properties.getCache();
        Properties properties = new Properties();
		properties.setProperty("metrics.define.testdm.value", "analysis(value, ana_props) == \"OK\"");
		properties.setProperty("metrics.define.testdm.metrics.groupby", "ALL");
		properties.setProperty("metrics.define.testdm.variables.value.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("metrics.define.testdm.variables.value.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("metrics.define.testdm.variables.ana_props.type", "fixed-threshold");
		properties.setProperty("metrics.define.testdm.variables.ana_props.error.upperbound", "90");
		properties.setProperty("monitor.mon1.filter.expr", "$defined_metric=testdm");
		properties.setProperty("monitor.mon1.analysis.type", "true");
		propertiesCache.set(properties);
		
		addInput(0,    Metric(0, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		addInput(1,    Metric(0, 91f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		addInput(2,    Metric(0, 89f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		Stream<Metric> metricsStream = createStream(Metric.class);
        
		Stream<Metric> definedMetrics = DefinedMetrics.generate(metricsStream, null, Optional.empty());
		Stream<AnalysisResult> results = Monitors.analyze(definedMetrics, null, Optional.empty());
        
        Batches<AnalysisResult> returnedBatches = collect(results);
        
        List<AnalysisResult> batch0 = returnedBatches.get(0);
        assertEquals(1, batch0.size());
        assertEquals(Status.OK, batch0.get(0).getStatus());
        
        List<AnalysisResult> batch1 = returnedBatches.get(1);
        assertEquals(1, batch1.size());
        assertEquals(Status.ERROR, batch1.get(0).getStatus());
        
        List<AnalysisResult> batch2 = returnedBatches.get(2);
        assertEquals(1, batch2.size());
        assertEquals(Status.OK, batch2.get(0).getStatus());
	}
	
	@Test
	public void shouldProduceExceptionAnaylisisResultWithConfigurationExceptionAndFilterOK() throws Exception {
        Cache<Properties> propertiesCache = Properties.getCache();
        Properties properties = new Properties();
		properties.setProperty("monitor.mon1.filter.expr", "HOST=host1");
		properties.setProperty("monitor.mon1.analysis.type", "does not exist");
		properties.setProperty("monitor.mon1.analysis.error.upperbound", "20");
		properties.setProperty("monitor.mon1.analysis.error.lowerbound", "10");
        propertiesCache.set(properties);
	        
        addInput(0,    Metric(0, 0, "HOST=host1"));
        addInput(0,    Metric(20, 0, "HOST=host2345"));
        addInput(0,    Metric(40, 0, "HOST=host1"));
        
        addInput(1,    Metric(60, 0, "HOST=host243"));
        addInput(1,    Metric(80, 0));
        
        addInput(2,    Metric(60, 0, "HOST=host243"));
        addInput(2,    Metric(80, 0));
        
        addInput(3,    Metric(100, 0, "HOST=host1"));
        addInput(3,    Metric(120, 0, "HOST=host1"));
        addInput(3,    Metric(140, 0));
        Stream<Metric> metricsStream = createStream(Metric.class);
        
		Stream<AnalysisResult> results = Monitors.analyze(metricsStream, null, Optional.empty());
        
        Batches<AnalysisResult> returnedBatches = collect(results);
        
        List<AnalysisResult> batch0 = returnedBatches.get(0);
        assertEquals(2, batch0.size());
        assertException("ConfigurationException: ANAYLSIS: Component class ", batch0.get(0));
        assertException("ConfigurationException: ANAYLSIS: Component class ", batch0.get(1));
        
        assertEquals(0, returnedBatches.get(1).size());
        
        assertEquals(0, returnedBatches.get(2).size());
        
        List<AnalysisResult> batch3 = returnedBatches.get(3);
        assertEquals(2, batch3.size());
        assertException("ConfigurationException: ANAYLSIS: Component class ", batch3.get(0));
        assertException("ConfigurationException: ANAYLSIS: Component class ", batch3.get(1));
	}

	@Test
	public void shouldProducePeriodicExceptionAnaylisisResultWithFilterConfigurationException() throws Exception {
        Cache<Properties> propertiesCache = Properties.getCache();
        Properties properties = new Properties();
        properties.setProperty("monitor.mon1.filter.expr", "does not work");
		properties.setProperty("monitor.mon1.analysis.type", "true");
		properties.setProperty("monitor.mon1.analysis.error.upperbound", "20");
		properties.setProperty("monitor.mon1.analysis.error.lowerbound", "10");
        propertiesCache.set(properties);
	        
        addInput(0,    Metric(0, 0, "HOST=host1"));
        addInput(0,    Metric(20, 0, "HOST=host2345"));
        addInput(0,    Metric(40, 0, "HOST=host3454"));
        
        addInput(1,    Metric(60, 0, "HOST=host243"));
        addInput(1,    Metric(80, 0));
        
        addInput(2,    Metric(82, 0, "HOST=host243"));
        addInput(2,    Metric(85, 0));
        
        addInput(3,    Metric(100, 0, "HOST=host5426"));
        addInput(3,    Metric(120, 0, "HOST=ho"));
        addInput(3,    Metric(140, 0));
        Stream<Metric> metricsStream = createStream(Metric.class);
        
		Stream<AnalysisResult> results = Monitors.analyze(metricsStream, null, Optional.empty());
        
        Batches<AnalysisResult> returnedBatches = collect(results);
        
        List<AnalysisResult> batch0 = returnedBatches.get(0);
        assertEquals(1, batch0.size());
        assertException("ConfigurationException: Error when parsing filter expression", batch0.get(0));
        
        List<AnalysisResult> batch1 = returnedBatches.get(1);
        assertEquals(1, batch1.size());
        assertException("ConfigurationException: Error when parsing filter expression", batch1.get(0));
        
        List<AnalysisResult> batch2 = returnedBatches.get(2);
        assertEquals(0, batch2.size());
        
        List<AnalysisResult> batch3 = returnedBatches.get(3);
        assertEquals(1, batch3.size());
        assertException("ConfigurationException: Error when parsing filter expression", batch3.get(0));
	}
	
	private void assertException(String exceptionStartsWith, AnalysisResult analysisResult) {
        assertEquals(Status.EXCEPTION, analysisResult.getStatus());
        
        assertTrue(analysisResult.getStatusReason().startsWith(exceptionStartsWith));
	}

}
