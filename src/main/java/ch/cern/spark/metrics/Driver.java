package ch.cern.spark.metrics;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.properties.source.PropertiesSource;
import ch.cern.spark.SparkConf;
import ch.cern.spark.metrics.defined.DefinedMetrics;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;
import ch.cern.spark.metrics.schema.MetricSchemas;
import ch.cern.spark.metrics.source.MetricsSource;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusesKeyReceiver;
import ch.cern.spark.status.storage.StatusesStorage;

public final class Driver {
    
    public static String BATCH_INTERVAL_PARAM = "spark.batch.time";
    
	public static String CHECKPOINT_DIR_PARAM = "checkpoint.dir";  
    public static String CHECKPOINT_DIR_DEFAULT = "/tmp/";  
    
    private JavaStreamingContext ssc;
    
	private List<MetricsSource> metricSources;
	private Optional<AnalysisResultsSink> analysisResultsSink;
	private List<NotificationsSink> notificationsSinks;

	public static String STATUSES_REMOVAL_SOCKET_PARAM = "statuses.removal.socket";
	private String statuses_removal_socket_host;
	private Integer statuses_removal_socket_port;

	public Driver(Properties properties) throws Exception {
		removeSparkCheckpointDir(properties.getProperty(CHECKPOINT_DIR_PARAM, CHECKPOINT_DIR_DEFAULT));
		
		String removalSocket = properties.getProperty(STATUSES_REMOVAL_SOCKET_PARAM);
		if(removalSocket != null) {
		    String[] host_port = removalSocket.trim().split(":");
		    
		    statuses_removal_socket_host = host_port[0];
		    statuses_removal_socket_port = Integer.parseInt(host_port[1]);
		}
		
        ssc = newStreamingContext(properties);

        metricSources = getMetricSources(properties);
		analysisResultsSink = getAnalysisResultsSink(properties);
		notificationsSinks = getNotificationsSinks(properties);
		
		if(!analysisResultsSink.isPresent() && notificationsSinks.size() == 0)
            throw new ConfigurationException("At least one sink must be configured");
	}

	public static void main(String[] args) throws Exception {

	    if(args.length != 1)
	        throw new ConfigurationException("A single argument must be specified with the path to the configuration file.");
	    
	    String propertyFilePath = args[0];
	    Properties properties = Properties.fromFile(propertyFilePath);
	    properties.setDefaultPropertiesSource(propertyFilePath);
	    
	    Driver driver = new Driver(properties);
		
        JavaStreamingContext ssc = driver.createNewStreamingContext(properties.getSubset(PropertiesSource.CONFIGURATION_PREFIX));
		
		// Start the computation
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

    private static void removeSparkCheckpointDir(String mainDir) throws IOException {
        Path path = new Path(mainDir + "/checkpoint/");
        
        FileSystem fs = FileSystem.get(new Configuration());
        
        if(fs.exists(path))
            fs.delete(path, true);
    }

    protected JavaStreamingContext createNewStreamingContext(Properties propertiesSourceProps) throws Exception {
	    
    		JavaDStream<Metric> metrics = getMetricStream(propertiesSourceProps);
    		
    		Optional<JavaDStream<StatusKey>> statusesToRemove = getStatusesToRemoveStream();
    		
		metrics = metrics.union(DefinedMetrics.generate(metrics, propertiesSourceProps, statusesToRemove));
		metrics.cache();
		
		JavaDStream<AnalysisResult> results = Monitors.analyze(metrics, propertiesSourceProps, statusesToRemove);
		results.cache();

		analysisResultsSink.ifPresent(sink -> sink.sink(results));
		
		JavaDStream<Notification> notifications = Monitors.notify(results, propertiesSourceProps, statusesToRemove);
		notifications.cache();
		
    		notificationsSinks.stream().forEach(sink -> sink.sink(notifications));
		
		return ssc;
	}

	private Optional<JavaDStream<StatusKey>> getStatusesToRemoveStream() {
	    if(statuses_removal_socket_host == null || statuses_removal_socket_port == null)
	        return Optional.empty();

        return Optional.of(ssc.receiverStream(new StatusesKeyReceiver(statuses_removal_socket_host, statuses_removal_socket_port)));
    }

    public JavaDStream<Metric> getMetricStream(Properties propertiesSourceProps) {
		return metricSources.stream()
				.map(source -> MetricSchemas.generate(source.createJavaDStream(ssc), propertiesSourceProps, source.getId(), source.getSchema()))
				.reduce((str, stro) -> str.union(stro)).get();
	}

	private Optional<AnalysisResultsSink> getAnalysisResultsSink(Properties properties) throws Exception {
		Properties analysisResultsSinkProperties = properties.getSubset("results.sink");
		
		return ComponentManager.buildOptional(Type.ANALYSIS_RESULTS_SINK, analysisResultsSinkProperties);
	}

	private List<MetricsSource> getMetricSources(Properties properties) throws Exception {
		List<MetricsSource> metricSources = new LinkedList<>();
		
    		Properties metricSourcesProperties = properties.getSubset("metrics.source");
		
    		Set<String> ids = metricSourcesProperties.getIDs();
    		
    		for (String id : ids) {
    			Properties props = metricSourcesProperties.getSubset(id);
			
    			MetricsSource source = ComponentManager.build(Type.METRIC_SOURCE, id, props);
    			source.setId(id);
    			
    			metricSources.add(source);
		}
    		
    		if(metricSources.size() < 1)
		    throw new ConfigurationException("At least one metric source must be configured");
		
		return metricSources;
	}
	
	private List<NotificationsSink> getNotificationsSinks(Properties properties) throws Exception {
		List<NotificationsSink> notificationsSinks = new LinkedList<>();
		
    		Properties sinkProperties = properties.getSubset("notifications.sink");
		
    		Set<String> ids = sinkProperties.getIDs();
    		
    		for (String id : ids) {
    			Properties props = sinkProperties.getSubset(id);
			
    			NotificationsSink sink = ComponentManager.build(Type.NOTIFICATIONS_SINK, props);
    			sink.setId(id);
    			
    			notificationsSinks.add(sink);
		}
		
		return notificationsSinks;
	}
	
	public JavaStreamingContext newStreamingContext(Properties properties) throws IOException, ConfigurationException {
		
		SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("MetricsMonitorStreamingJob");
        sparkConf.runLocallyIfMasterIsNotConfigured();
        sparkConf.addProperties(properties, "spark.");
        
        String checkpointDir = properties.getProperty(CHECKPOINT_DIR_PARAM, CHECKPOINT_DIR_DEFAULT);
        
        if(!sparkConf.contains(StatusesStorage.STATUS_STORAGE_PARAM + ".type")) {
        		sparkConf.set(StatusesStorage.STATUS_STORAGE_PARAM + ".type", "single-file");
        		sparkConf.set(StatusesStorage.STATUS_STORAGE_PARAM + ".path", checkpointDir + "/statuses");
        }

    		long batchInterval = properties.getPeriod(BATCH_INTERVAL_PARAM, Duration.ofMinutes(1)).getSeconds();
		
    		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
		
		ssc.checkpoint(checkpointDir + "/checkpoint/");
		
		return ssc;
	}
	
	public JavaStreamingContext getJavaStreamingContext() {
		return ssc;
	}

}
