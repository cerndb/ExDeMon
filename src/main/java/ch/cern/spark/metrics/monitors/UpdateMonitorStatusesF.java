package ch.cern.spark.metrics.monitors;

import java.util.Optional;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.UpdateStatusFunction;

public class UpdateMonitorStatusesF extends UpdateStatusFunction<MonitorStatusKey, Metric, StatusValue, AnalysisResult> {

    private static final long serialVersionUID = 3156649511706333348L;
    
    private Properties propertiesSourceProperties;
    
    public UpdateMonitorStatusesF(Properties propertiesSourceProperties) {
    		this.propertiesSourceProperties = propertiesSourceProperties;
    }
    
    @Override
    protected Optional<AnalysisResult> update(Time time, MonitorStatusKey ids, Metric metric, State<StatusValue> status)
            throws Exception {
        Monitors.initCache(propertiesSourceProperties);
        
        Optional<Monitor> monitorOpt = Optional.ofNullable(Monitors.getCache().get().get(ids.getMonitorID()));
        if(!monitorOpt.isPresent()) {
            status.remove();
            
            return Optional.empty();
        }
        
        Monitor monitor = monitorOpt.get();
        
        return monitor.process(status, metric, time);
    }

}
