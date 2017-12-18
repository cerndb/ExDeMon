package ch.cern.spark.metrics.monitors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.log4j.Logger;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notificator.ComputeNotificatorKeysF;
import ch.cern.spark.metrics.notificator.NotificatorStatusKey;
import ch.cern.spark.metrics.notificator.UpdateNotificatorStatusesF;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;

public class Monitors {

	private transient final static Logger LOG = Logger.getLogger(Monitors.class.getName());
	
	private static Cache<Map<String, Monitor>> cachedMonitors = new Cache<Map<String,Monitor>>() {
		
		@Override
		protected Map<String, Monitor> load() throws Exception {
	        Properties properties = Properties.getCache().get().getSubset("monitor");
	        
	        Set<String> monitorNames = properties.getUniqueKeyFields();
	        
	        Map<String, Monitor> monitors = new HashMap<>();
	        for (String monitorName : monitorNames) {
				Properties monitorProps = properties.getSubset(monitorName);
				
				monitors.put(monitorName, new Monitor(monitorName).config(monitorProps));
			}

	        LOG.info("Loaded Monitors: " + monitors);
	        
	        return monitors;
		}
	};
	
	public static Stream<AnalysisResult> analyze(Stream<Metric> metrics, Properties propertiesSourceProps, Optional<Stream<StatusKey>> allStatusesToRemove) throws Exception {
	    Stream<MonitorStatusKey> statusesToRemove = null;
	    if(allStatusesToRemove.isPresent())
	        statusesToRemove = allStatusesToRemove.get()
	                                .filter(key -> key instanceof MonitorStatusKey)
	                                .map(key -> (MonitorStatusKey) key);
	    
        return metrics.mapWithState(
                            MonitorStatusKey.class, 
                            StatusValue.class, 
                            new ComputeMonitorKeysF(propertiesSourceProps), 
                            Optional.ofNullable(statusesToRemove),
                            new UpdateMonitorStatusesF(propertiesSourceProps));
	}

	public static Stream<Notification> notify(Stream<AnalysisResult> results, Properties propertiesSourceProps, Optional<Stream<StatusKey>> allStatusesToRemove) throws IOException, ClassNotFoundException, ConfigurationException {
	    Stream<NotificatorStatusKey> statusesToRemove = null;
	    if(allStatusesToRemove.isPresent())
	        statusesToRemove = allStatusesToRemove.get()
	                                    .filter(key -> key instanceof NotificatorStatusKey)
	                                    .map(key -> (NotificatorStatusKey) key);
	        
        return results.mapWithState(
                            NotificatorStatusKey.class, 
                            StatusValue.class, 
                            new ComputeNotificatorKeysF(propertiesSourceProps), 
                            Optional.ofNullable(statusesToRemove),
                            new UpdateNotificatorStatusesF(propertiesSourceProps));
	}
	
	public static Cache<Map<String, Monitor>> getCache() {
		return cachedMonitors;
	}

	public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
		Properties.initCache(propertiesSourceProps);
		
		getCache().setExpiration(Properties.getCache().getExpirationPeriod());
	}

}
