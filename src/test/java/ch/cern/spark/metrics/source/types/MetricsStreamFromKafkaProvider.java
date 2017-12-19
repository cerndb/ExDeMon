package ch.cern.spark.metrics.source.types;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.apache.spark.util.ManualClock;
import org.junit.After;
import org.junit.Before;

import ch.cern.properties.Properties;
import ch.cern.spark.BatchCounter;
import ch.cern.spark.SparkConf;
import ch.cern.spark.json.JSONParser;
import ch.cern.spark.metrics.JSONMetric;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.schema.MetricSchemas;
import ch.cern.spark.status.storage.StatusesStorage;

public class MetricsStreamFromKafkaProvider implements Serializable{

	private static final long serialVersionUID = 1857762447792729837L;
	
	private transient JavaStreamingContext sc = null;
	public transient BatchCounter batchCounter;

	private transient KafkaTestUtils kafkaTestUtils;
	private KafkaMetricsSource metricsSource;
	private String topic;

	@Before
	public void setUp() throws Exception {
		kafkaTestUtils = new KafkaTestUtils();
		kafkaTestUtils.setup();

		topic = "items-topic";
		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put(topic, 1);
		kafkaTestUtils.createTopic(topic);
	
		Properties properties = new Properties();
		properties.setProperty("type", "kafka");
		properties.setProperty("topics", topic);
		properties.setProperty("consumer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.group.id", "testing");
		properties.setProperty("schema.attributes", "CLUSTER HOSTNAME METRIC KEY_TO_REMOVE");
		properties.setProperty("schema.value.keys", "VALUE");
		properties.setProperty("schema.timestamp.key", "TIMESTAMP");
		properties.setProperty("schema.filter.attribute.KEY_TO_REMOVE", "!.*");
		metricsSource = new KafkaMetricsSource();
		metricsSource.setId("kafka");
		metricsSource.config(properties);
		
		Path checkpointPath = new Path("/tmp/spark-checkpoint-tests/");
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(checkpointPath, true);
		
		SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Test");
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.driver.host", "localhost");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        sparkConf.set("spark.streaming.clock", "org.apache.spark.util.ManualClock");
        sparkConf.set(StatusesStorage.STATUS_STORAGE_PARAM + ".type", "single-file");
        sparkConf.set(StatusesStorage.STATUS_STORAGE_PARAM + ".path", checkpointPath.toString());
        
    		sc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
    		sc.checkpoint(checkpointPath.toString());
    		
		batchCounter = new BatchCounter();
		sc.addStreamingListener(batchCounter);
	}
	
	public JavaDStream<Metric> createStream(){
		return MetricSchemas.generate(metricsSource.createJavaDStream(sc), null, metricsSource.getId(), metricsSource.getSchema());
	}
	
	public void sendMetrics(List<Metric> inputMetrics) {
		inputMetrics = inputMetrics.stream().map(m -> {
							m.getIDs().put("$source", "kafka");
							
							return m;
						}).collect(Collectors.toList());
		
		List<String> asStrings = toJSON(inputMetrics);
		
		kafkaTestUtils.sendMessages(topic, asStrings.toArray(new String[0]));
	}
	
	public boolean start(int numBatches){
		sc.start();
		
		((ManualClock) sc.ssc().scheduler().clock()).advance(sc.ssc().progressListener().batchDuration() * numBatches + 1);
		
		return true;
	}
	
	public boolean waitUntilBatchesCompleted(int expectedNumCompletedBatches){
		long now = System.currentTimeMillis();
		long batchDuration = sc.ssc().progressListener().batchDuration();
		long timeout = now + (expectedNumCompletedBatches * batchDuration) + 5000; 
		
		while(now < timeout) {
			if(batchCounter.getNumCompletedBatches() < expectedNumCompletedBatches) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {}
			}else {
				return true;
			}
			
			now = System.currentTimeMillis();
		}
		
		return false;
	}
	
	@After
	public void tearDown() {
		if(sc != null)
			sc.stop();
		sc = null;
	}
	
	private List<String> toJSON(List<Metric> input) {
		return input.stream()
				.map(metric -> JSONParser.parse(new JSONMetric(metric)).toString())
				.collect(Collectors.toList());
	}
	
}
