package ch.cern.spark.metrics.defined;

import java.util.Optional;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.status.UpdateStatusFunction;

public class UpdateDefinedMetricStatusesF extends UpdateStatusFunction<DefinedMetricStatuskey, Metric, VariableStatuses, Metric>{

	private static final long serialVersionUID = 2965182980222300453L;

	private Properties propertiesSourceProps;

	public UpdateDefinedMetricStatusesF(Properties propertiesSourceProps) {
		this.propertiesSourceProps = propertiesSourceProps;
	}
	
    @Override
    protected Optional<Metric> update(Time time, DefinedMetricStatuskey id, Metric metric, State<VariableStatuses> status) 
            throws Exception {
        DefinedMetrics.initCache(propertiesSourceProps);
        
        Optional<DefinedMetric> definedMetricOpt = Optional.of(DefinedMetrics.getCache().get().get(id.getDefinedMetricName()));
        if(!definedMetricOpt.isPresent()) {
            status.remove();
            return Optional.empty();
        }
        DefinedMetric definedMetric = definedMetricOpt.get();
            
        VariableStatuses store = getStore(status);

        definedMetric.updateStore(store, metric, id.getGroupByMetricIDs().keySet());
        
        Optional<Metric> newMetric = definedMetric.generateByUpdate(store, metric, id.getGroupByMetricIDs());
        
        store.update(status, time);
        
        return newMetric;
    }

	private VariableStatuses getStore(State<VariableStatuses> status) {
		return status.exists() ? status.get() : new VariableStatuses();
	}

}
