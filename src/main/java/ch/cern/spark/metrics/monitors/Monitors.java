package ch.cern.spark.metrics.monitors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notificator.ComputeNotificatorKeysF;
import ch.cern.spark.metrics.notificator.NotificatorStatusKey;
import ch.cern.spark.metrics.notificator.UpdateNotificatorStatusesF;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.status.State;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;

public class Monitors {

	private transient final static Logger LOG = Logger.getLogger(Monitors.class.getName());
	
	private static Cache<Map<String, Monitor>> cachedMonitors = new Cache<Map<String,Monitor>>() {
		
		@Override
		protected Map<String, Monitor> load() throws Exception {
	        Properties properties = Properties.getCache().get().getSubset("monitor");
	        
	        Set<String> monitorNames = properties.getIDs();
	        
	        Map<String, Monitor> monitors = new HashMap<>();
	        for (String monitorName : monitorNames) {
				Properties monitorProps = properties.getSubset(monitorName);
				
				monitors.put(monitorName, new Monitor(monitorName).config(monitorProps));
			}

	        LOG.info("Loaded Monitors: " + monitors);
	        
	        return monitors;
		}
	};
	
	public static JavaDStream<AnalysisResult> analyze(
	        JavaDStream<Metric> metrics, 
	        Properties propertiesSourceProps, 
	        Optional<JavaDStream<StatusKey>> allStatusesToRemove) 
	                throws Exception {
	    
	    JavaDStream<MonitorStatusKey> statusesToRemove = null;
	    if(allStatusesToRemove.isPresent())
	        statusesToRemove = allStatusesToRemove.get()
	                                .filter(key -> key instanceof MonitorStatusKey)
	                                .map(key -> (MonitorStatusKey) key);
	    
	    JavaPairDStream<MonitorStatusKey, Metric> idAndMetrics = metrics.flatMapToPair(new ComputeMonitorKeysF(propertiesSourceProps));

	    return State.<MonitorStatusKey, Metric, StatusValue, AnalysisResult>map(
	                    MonitorStatusKey.class, 
	                    StatusValue.class, 
	                    idAndMetrics, 
	                    new UpdateMonitorStatusesF(propertiesSourceProps),
	                    Optional.ofNullable(statusesToRemove));
	}

	public static JavaDStream<Notification> notify(JavaDStream<AnalysisResult> results, Properties propertiesSourceProps, Optional<JavaDStream<StatusKey>> allStatusesToRemove) throws IOException, ClassNotFoundException, ConfigurationException {
	    JavaDStream<NotificatorStatusKey> statusesToRemove = null;
	    if(allStatusesToRemove.isPresent())
	        statusesToRemove = allStatusesToRemove.get()
	                                    .filter(key -> key instanceof NotificatorStatusKey)
	                                    .map(key -> (NotificatorStatusKey) key);
	    
	    JavaPairDStream<NotificatorStatusKey, AnalysisResult> idAndAnalysis = results.flatMapToPair(new ComputeNotificatorKeysF(propertiesSourceProps));
	    
	    return State.<NotificatorStatusKey, AnalysisResult, StatusValue, Notification>map(
	                    NotificatorStatusKey.class, 
                        StatusValue.class, 
                        idAndAnalysis, 
                        new UpdateNotificatorStatusesF(propertiesSourceProps),
                        Optional.ofNullable(statusesToRemove));
	}
	
	public static Cache<Map<String, Monitor>> getCache() {
		return cachedMonitors;
	}

	public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
		Properties.initCache(propertiesSourceProps);
		
		getCache().setExpiration(Properties.getCache().getExpirationPeriod());
	}

}
