package ch.cern.spark.metrics.monitors;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.State;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.analysis.Analysis;
import ch.cern.spark.metrics.analysis.types.NoneAnalysis;
import ch.cern.spark.metrics.filter.MetricsFilter;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.trigger.Trigger;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.StatusValue;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class Monitor {
    
    private final static Logger LOG = Logger.getLogger(Monitor.class.getName());
    
    @Getter @Setter
    protected String id;
    
    @Getter
    private MetricsFilter filter;

    private Analysis analysis;
    
    @Getter
    protected Map<String, Trigger> triggers;

	private Map<String, String> tags;
    
    public Monitor(String id){
        this.id = id;
    }
    
    public Monitor config(Properties properties) {
    		try {
			return tryConfig(properties);
		} catch (Exception e) {
		    LOG.error(id + ": " + e.getMessage(), e);
		    
			InErrorMonitor errorMonitor = new InErrorMonitor(id, e);
			
			return errorMonitor.config(properties);
		}
    }
    
	public Monitor tryConfig(Properties properties) throws ConfigurationException {
        filter = MetricsFilter.build(properties.getSubset("filter"));
        
        Properties analysis_props = properties.getSubset("analysis");
        if(!analysis_props.isTypeDefined())
        		analysis_props.setProperty("type", NoneAnalysis.class.getAnnotation(RegisterComponent.class).value());
    		analysis = ComponentManager.build(Type.ANAYLSIS, analysis_props);
        
    		Properties triggersProps = properties.getSubset("triggers");
    		
        //TODO backward compatibility
    		Properties notificatorsPropsOld = properties.getSubset("notificator");
    		triggersProps.putAll(notificatorsPropsOld);
        //TODO backward compatibility
        
        Set<String> triggerIds = triggersProps.getIDs();
        triggers = new HashMap<>();
        for (String triggerId : triggerIds) {
        		Properties props = triggersProps.getSubset(triggerId);
        		
        		if(!props.isTypeDefined())
        		    props.setProperty("type", "statuses");
        		
        		triggers.put(triggerId, ComponentManager.build(Type.TRIGGER, triggerId, props));
		}
        
        tags = properties.getSubset("tags").toStringMap();
        
        properties.confirmAllPropertiesUsed();
        
        return this;
    }

    public Optional<AnalysisResult> process(State<StatusValue> status, Metric metric) {
    		AnalysisResult result = null;

        try{
        		if(analysis.hasStatus() && status.exists())
            		((HasStatus) analysis).load(status.get());
        		
            result = analysis.apply(metric);
            
            result.addAnalysisParam("type", analysis.getClass().getAnnotation(RegisterComponent.class).value());
            
            if(analysis.hasStatus())
            		analysis.getStatus().ifPresent(s -> status.update(s));
        }catch(Throwable e){
            result = AnalysisResult.buildWithStatus(Status.EXCEPTION, e.getClass().getSimpleName() + ": " + e.getMessage());
            LOG.error(e.getMessage(), e);
        }
        
        metric.addAttribute("$monitor", id);
        result.addAnalysisParam("monitor.name", id);
        result.setAnalyzedMetric(metric);
        result.setTags(tags);

        return Optional.of(result);
    }
    
    public Map<String, String> getMetricIDs(Metric metric) {
		return metric.getAttributes();
	}

}
