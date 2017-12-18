package ch.cern.spark.status.storage.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.StateImpl;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.RDD;
import ch.cern.spark.SparkConf;
import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.monitors.MonitorStatusKey;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.TestStatus;
import scala.Tuple2;

public class KafkaStatusesStorageTest {
	
	private transient JavaSparkContext context = null;

	private transient KafkaTestUtils kafkaTestUtils;
	private String topic;
	
	@Before
	public void setUp() throws Exception {
		kafkaTestUtils = new KafkaTestUtils();
		kafkaTestUtils.setup();

		topic = "test";
		kafkaTestUtils.createTopic(topic);
		
		SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Test");
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.driver.host", "localhost");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        
    		context = new JavaSparkContext(sparkConf);
	}
	
	@Test
	public void shouldSaveAndLoadSameData() throws Exception {
		KafkaStatusesStorage storage = new KafkaStatusesStorage();
		
		Properties properties = new Properties();
		properties.setProperty("topic", topic);
		properties.setProperty("producer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.group.id", "testing1");
		storage.config(properties);
		
		List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> inputList = new LinkedList<>();
		DefinedMetricStatuskey id = new DefinedMetricStatuskey("df1", new HashMap<>());
		VariableStatuses statuses = new VariableStatuses();
		statuses.newProcessedBatchTime(Instant.now());
		statuses.put("v1", new TestStatus(100));
		statuses.put("v2", new TestStatus(101));
		inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, statuses));
		
		RDD<Tuple2<DefinedMetricStatuskey, VariableStatuses>> inputRDD = RDD.from(context.parallelize(inputList));
		storage.save(inputRDD, new Time(0));
		
		JavaRDD<Tuple2<DefinedMetricStatuskey, VariableStatuses>> outputRDD = storage.load(context, DefinedMetricStatuskey.class, VariableStatuses.class);
		List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> outputList = outputRDD.collect();
		
		assertNotSame(inputList, outputList);
		assertEquals(inputList, outputList);
	}
	
	@Test
	public void shouldLoadDifferentKeys() throws Exception {
		KafkaStatusesStorage storage = new KafkaStatusesStorage();
		
		Properties properties = new Properties();
		properties.setProperty("topic", topic);
		properties.setProperty("producer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.group.id", "testing2");
		storage.config(properties);
		
		List<Tuple2<StatusKey, StatusValue>> inputList = new LinkedList<>();
		
		StatusKey id = new DefinedMetricStatuskey("df1", new HashMap<>());
		StatusValue status = new TestStatus(14);
		Tuple2<StatusKey, StatusValue> tuple1 = new Tuple2<StatusKey, StatusValue>(id, status);
		inputList.add(tuple1);
		RDD<Tuple2<StatusKey, StatusValue>> inputRDD = RDD.from(context.parallelize(inputList));
		storage.save(inputRDD, new Time(0));
		
		inputList = new LinkedList<>();
		id = new MonitorStatusKey("mon1", new HashMap<>());
		status = new TestStatus(10);
		Tuple2<StatusKey, StatusValue> tuple2 = new Tuple2<StatusKey, StatusValue>(id, status);
		inputList.add(tuple2);
		inputRDD = RDD.from(context.parallelize(inputList));
		
		storage.save(inputRDD, new Time(0));
		
		JavaRDD<Tuple2<DefinedMetricStatuskey, TestStatus>> outputRDD = storage.load(context, DefinedMetricStatuskey.class, TestStatus.class);
		List<Tuple2<DefinedMetricStatuskey, TestStatus>> outputList = outputRDD.collect();
		
		JavaRDD<Tuple2<MonitorStatusKey, TestStatus>> outputRDD2 = storage.load(context, MonitorStatusKey.class, TestStatus.class);
		List<Tuple2<MonitorStatusKey, TestStatus>> outputList2 = outputRDD2.collect();
		
		assertEquals(tuple1, outputList.get(0));
		assertEquals(tuple2, outputList2.get(0));
	}
	
	@Test
	public void shouldGetLastRecord() throws Exception {
		KafkaStatusesStorage storage = new KafkaStatusesStorage();
		
		Properties properties = new Properties();
		properties.setProperty("topic", topic);
		properties.setProperty("producer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.group.id", "testing3");
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
		
		RDD<Tuple2<DefinedMetricStatuskey, VariableStatuses>> inputRDD = RDD.from(context.parallelize(inputList));
		storage.save(inputRDD, new Time(0));
		inputList = new LinkedList<>();
		
		varStatuses = new VariableStatuses();
		status = new TestStatus(12);
		varStatuses.put("var2-id1", status);
		inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id1, varStatuses));
		
		inputRDD = RDD.from(context.parallelize(inputList));
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
		
		inputRDD = RDD.from(context.parallelize(inputList));
		storage.save(inputRDD, new Time(0));
		
		JavaRDD<Tuple2<DefinedMetricStatuskey, VariableStatuses>> outputRDD = storage.load(context, DefinedMetricStatuskey.class, VariableStatuses.class);
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
	
    @Test
    public void shouldNotGetLastRecordWhenValueNull() throws Exception {
        KafkaStatusesStorage storage = new KafkaStatusesStorage();

        Properties properties = new Properties();
        properties.setProperty("topic", topic);
        properties.setProperty("producer.bootstrap.servers", kafkaTestUtils.brokerAddress());
        properties.setProperty("consumer.bootstrap.servers", kafkaTestUtils.brokerAddress());
        properties.setProperty("consumer.group.id", "testing3");
        storage.config(properties);

        List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> inputList = new LinkedList<>();

        DefinedMetricStatuskey id1 = new DefinedMetricStatuskey("df1", new HashMap<>());

        VariableStatuses varStatuses = new VariableStatuses();
        StatusValue status = new TestStatus(11);
        varStatuses.put("var1-id1", status);
        inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id1, varStatuses));

        RDD<Tuple2<DefinedMetricStatuskey, VariableStatuses>> inputRDD = RDD.from(context.parallelize(inputList));
        storage.save(inputRDD, new Time(0));
        inputList = new LinkedList<>();

        varStatuses = new VariableStatuses();
        status = new TestStatus(12);
        varStatuses.put("var2-id1", status);
        inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id1, varStatuses));

        inputRDD = RDD.from(context.parallelize(inputList));
        storage.save(inputRDD, new Time(0));
        inputList = new LinkedList<>();

        inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id1, null));

        inputRDD = RDD.from(context.parallelize(inputList));
        storage.save(inputRDD, new Time(0));

        JavaRDD<Tuple2<DefinedMetricStatuskey, VariableStatuses>> outputRDD = storage.load(context,
                DefinedMetricStatuskey.class, VariableStatuses.class);
        List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> outputList = outputRDD.collect();
        
        assertEquals(0, outputList.size());
    }
	
	@Test
	public void shouldSaveOnlyRecordsOfBatchTime() throws Exception {
		KafkaStatusesStorage storage = new KafkaStatusesStorage();
		
		Properties properties = new Properties();
		properties.setProperty("topic", topic);
		properties.setProperty("producer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.group.id", "testing4");
		storage.config(properties);
		
		List<Tuple2<DefinedMetricStatuskey, TestStatus>> inputList = new LinkedList<>();
		
		DefinedMetricStatuskey id = new DefinedMetricStatuskey("df1", new HashMap<>());
		
		TestStatus status = new TestStatus(10);
		status.update(new StateImpl<>(), new Time(1));
		inputList.add(new Tuple2<DefinedMetricStatuskey, TestStatus>(id, status));
		
		RDD<Tuple2<DefinedMetricStatuskey, TestStatus>> inputRDD = RDD.from(context.parallelize(inputList));
		storage.save(inputRDD, new Time(1));
		inputList = new LinkedList<>();
		
		
		status = new TestStatus(11);
		status.update(new StateImpl<>(), new Time(2));
		inputList.add(new Tuple2<DefinedMetricStatuskey, TestStatus>(id, status));
		
		inputRDD = RDD.from(context.parallelize(inputList));
		storage.save(inputRDD, new Time(0));
		inputList = new LinkedList<>();
		
		
		status = new TestStatus(12);
		status.update(new StateImpl<>(), new Time(3));
		inputList.add(new Tuple2<DefinedMetricStatuskey, TestStatus>(id, status));
		
		inputRDD = RDD.from(context.parallelize(inputList));
		storage.save(inputRDD, new Time(0));
		
		JavaRDD<Tuple2<DefinedMetricStatuskey, TestStatus>> outputRDD = storage.load(context, DefinedMetricStatuskey.class, TestStatus.class);
		List<Tuple2<DefinedMetricStatuskey, TestStatus>> outputList = outputRDD.collect();
		
		List<Tuple2<DefinedMetricStatuskey, TestStatus>> expectedList = new LinkedList<>();
		status = new TestStatus(10);
		status.update(new StateImpl<>(), new Time(1));
		expectedList.add(new Tuple2<DefinedMetricStatuskey, TestStatus>(id, status));
		
		assertEquals(expectedList, outputList);
	}

}
