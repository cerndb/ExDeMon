package ch.cern.spark.metrics.defined;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.status.ActionOrValue;
import ch.cern.spark.status.State;
import ch.cern.spark.status.StatusKey;
import ch.cern.utils.Pair;

public class DefinedMetrics {

	private transient final static Logger LOG = Logger.getLogger(DefinedMetrics.class.getName());
	
	private static final Cache<Map<String, DefinedMetric>> cachedDefinedMetrics = new Cache<Map<String,DefinedMetric>>() {
		
		@Override
		protected Map<String, DefinedMetric> load() throws Exception {
	        Properties properties = Properties.getCache().get().getSubset("metrics.define");
	        
	        Set<String> metricsDefinedNames = properties.getIDs();
	        
	        Map<String, DefinedMetric> definedMetrics = metricsDefinedNames.stream()
	        		.map(id -> new Pair<String, Properties>(id, properties.getSubset(id)))
	        		.map(info -> new DefinedMetric(info.first).config(info.second))
	        		.filter(out -> out != null)
	        		.collect(Collectors.toMap(DefinedMetric::getName, m -> m));
	        
	        LOG.info("Metrics defined: " + definedMetrics);
	        
	        return definedMetrics;
		}
	};
	
	public static JavaDStream<Metric> generate(JavaDStream<Metric> metrics, Properties propertiesSourceProps, Optional<JavaDStream<StatusKey>> allStatusesToRemove) throws ClassNotFoundException, IOException, ConfigurationException{
	    JavaDStream<DefinedMetricStatuskey> statusesToRemove = null;
        if(allStatusesToRemove.isPresent())
            statusesToRemove = allStatusesToRemove.get()
                                    .filter(key -> key instanceof DefinedMetricStatuskey)
                                    .map(key -> (DefinedMetricStatuskey) key);

        JavaPairDStream<DefinedMetricStatuskey, Metric> idAndMetrics = metrics.flatMapToPair(new ComputeDefinedMetricKeysF(propertiesSourceProps));
        
        JavaMapWithStateDStream<DefinedMetricStatuskey, ActionOrValue<Metric>, VariableStatuses, Metric> statuses = 
                State.<DefinedMetricStatuskey, Metric, VariableStatuses, Metric>map(
                        DefinedMetricStatuskey.class, 
                        VariableStatuses.class, 
                        idAndMetrics, 
                        new UpdateDefinedMetricStatusesF(propertiesSourceProps),
                        Optional.ofNullable(statusesToRemove));

		JavaDStream<Metric> definedMetricsWhenBatch = statuses.stateSnapshots().transform((rdd, time) -> rdd.flatMap(new ComputeBatchDefineMetricsF(time, propertiesSourceProps)));
        
        return statuses.union(definedMetricsWhenBatch); 
	}
	
	public static Cache<Map<String, DefinedMetric>> getCache() {
		return cachedDefinedMetrics;
	}

	public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
		Properties.initCache(propertiesSourceProps);
	
		getCache().setExpiration(Properties.getCache().getExpirationPeriod());
	}

}
