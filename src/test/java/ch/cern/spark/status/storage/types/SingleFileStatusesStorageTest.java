package ch.cern.spark.status.storage.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Time;
import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.SparkConf;
import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.monitors.MonitorStatusKey;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.TestStatus;
import scala.Tuple2;

public class SingleFileStatusesStorageTest {
	
	private transient JavaSparkContext context = null;

	private static transient FileSystem fs = null;
	
	private String path;
	
	@Before
	public void setUp() throws Exception {
		path = "/tmp/spark-metrics-monitor-tests/SingleFileStatusesStorageTest/";
		
		setFileSystem();
		fs.delete(new Path(path), true);
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Test");
		sparkConf.setMaster("local[2]");
		sparkConf.set("spark.driver.host", "localhost");
		sparkConf.set("spark.driver.allowMultipleContexts", "true");

		context = new JavaSparkContext(sparkConf);
	}
	
	@Test
	public void shouldSaveAndLoadSameData() throws Exception {
		SingleFileStatusesStorage storage = new SingleFileStatusesStorage();
		
		Properties properties = new Properties();
		properties.setProperty("path", path);
		storage.config(properties);
		
		List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> inputList = new LinkedList<>();
		DefinedMetricStatuskey id = new DefinedMetricStatuskey("df1", new HashMap<>());
		VariableStatuses statuses = new VariableStatuses();
		statuses.newProcessedBatchTime(Instant.now());
		statuses.put("v1", new TestStatus(100));
		statuses.put("v2", new TestStatus(101));
		inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, statuses));
		
		JavaPairRDD<DefinedMetricStatuskey, VariableStatuses> inputRDD = context.parallelize(inputList).mapToPair(f->f);;
		storage.save(inputRDD, new Time(0));
		
		JavaPairRDD<DefinedMetricStatuskey, VariableStatuses> outputRDD = storage.load(context, DefinedMetricStatuskey.class, VariableStatuses.class);
		List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> outputList = outputRDD.collect();
		
		assertNotSame(inputList, outputList);
		assertEquals(inputList, outputList);
	}
	
	@Test
	public void shouldLoadDifferentKeys() throws Exception {
		SingleFileStatusesStorage storage = new SingleFileStatusesStorage();
		
		Properties properties = new Properties();
		properties.setProperty("path", path);
		storage.config(properties);
		
		List<Tuple2<StatusKey, StatusValue>> inputList = new LinkedList<>();
		
		StatusKey id = new DefinedMetricStatuskey("df1", new HashMap<>());
		StatusValue status = new TestStatus(14);
		Tuple2<StatusKey, StatusValue> tuple1 = new Tuple2<StatusKey, StatusValue>(id, status);
		inputList.add(tuple1);
		JavaPairRDD<StatusKey, StatusValue> inputRDD = context.parallelize(inputList).mapToPair(f->f);
		storage.save(inputRDD, new Time(0));
		
		inputList = new LinkedList<>();
		id = new MonitorStatusKey("mon1", new HashMap<>());
		status = new TestStatus(10);
		Tuple2<StatusKey, StatusValue> tuple2 = new Tuple2<StatusKey, StatusValue>(id, status);
		inputList.add(tuple2);
		inputRDD = context.parallelize(inputList).mapToPair(f->f);
		storage.save(inputRDD, new Time(0));
		
		JavaPairRDD<DefinedMetricStatuskey, TestStatus> outputRDD = storage.load(context, DefinedMetricStatuskey.class, TestStatus.class);
		List<Tuple2<DefinedMetricStatuskey, TestStatus>> outputList = outputRDD.collect();
		
		JavaPairRDD<MonitorStatusKey, TestStatus> outputRDD2 = storage.load(context, MonitorStatusKey.class, TestStatus.class);
		List<Tuple2<MonitorStatusKey, TestStatus>> outputList2 = outputRDD2.collect();
		
		assertEquals(tuple1, outputList.get(0));
		assertEquals(tuple2, outputList2.get(0));
	}
	
	@Test
	public void shouldGetLastRecord() throws Exception {
		SingleFileStatusesStorage storage = new SingleFileStatusesStorage();
		
		Properties properties = new Properties();
		properties.setProperty("path", path);
		storage.config(properties);
		
		List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> inputList = new LinkedList<>();
		
		DefinedMetricStatuskey id1 = new DefinedMetricStatuskey("df1", new HashMap<>());
		DefinedMetricStatuskey id2 = new DefinedMetricStatuskey("df2", new HashMap<>());
		
		VariableStatuses varStatuses = new VariableStatuses();
		StatusValue status = new TestStatus(11);
		varStatuses.put("var1-id1", status);
		inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id1, varStatuses));
		
		varStatuses = new VariableStatuses();
		status = new TestStatus(12);
		varStatuses.put("var2-id2", status);
		inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id2, varStatuses));

		JavaPairRDD<DefinedMetricStatuskey, VariableStatuses> inputRDD = context.parallelize(inputList).mapToPair(f->f);
		storage.save(inputRDD, new Time(0));
		
		
		inputList = new LinkedList<>();
		
		varStatuses = new VariableStatuses();
		status = new TestStatus(13);
		varStatuses.put("var3-id1", status);
		inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id1, varStatuses));
		
		varStatuses = new VariableStatuses();
		status = new TestStatus(12);
		varStatuses.put("var3-id2", status);
		inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id2, varStatuses));
		
		inputRDD = context.parallelize(inputList).mapToPair(f->f);
		storage.save(inputRDD, new Time(0));
		
		
		
		JavaPairRDD<DefinedMetricStatuskey, VariableStatuses> outputRDD = storage.load(context, DefinedMetricStatuskey.class, VariableStatuses.class);
		List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> outputList = outputRDD.collect();
		
		List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> expectedList = new LinkedList<>();
		varStatuses = new VariableStatuses();
		status = new TestStatus(13);
		varStatuses.put("var3-id1", status);
		expectedList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id1, varStatuses));
		
		varStatuses = new VariableStatuses();
		status = new TestStatus(12);
		varStatuses.put("var3-id2", status);
		expectedList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id2, varStatuses));
		
		assertEquals(expectedList, outputList);
	}
	
	private void setFileSystem() throws IOException {
		if (fs == null)
			fs = FileSystem.get(new Configuration());
	}

}
