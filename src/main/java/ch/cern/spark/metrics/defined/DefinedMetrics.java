package ch.cern.spark.metrics.defined;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.Pair;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusStream;

public class DefinedMetrics {

	private transient final static Logger LOG = Logger.getLogger(DefinedMetrics.class.getName());
	
	private static final Cache<Map<String, DefinedMetric>> cachedDefinedMetrics = new Cache<Map<String,DefinedMetric>>() {
		
		@Override
		protected Map<String, DefinedMetric> load() throws Exception {
	        Properties properties = Properties.getCache().get().getSubset("metrics.define");
	        
	        Set<String> metricsDefinedNames = properties.getUniqueKeyFields();
	        
	        Map<String, DefinedMetric> definedMetrics = metricsDefinedNames.stream()
	        		.map(id -> new Pair<String, Properties>(id, properties.getSubset(id)))
	        		.map(info -> new DefinedMetric(info.first).config(info.second))
	        		.filter(out -> out != null)
	        		.collect(Collectors.toMap(DefinedMetric::getName, m -> m));
	        
	        LOG.info("Metrics defined: " + definedMetrics);
	        
	        return definedMetrics;
		}
	};
	
	public static Stream<Metric> generate(Stream<Metric> metrics, Properties propertiesSourceProps, Optional<Stream<StatusKey>> allStatusesToRemove) throws ClassNotFoundException, IOException, ConfigurationException{
	    Stream<DefinedMetricStatuskey> statusesToRemove = null;
        if(allStatusesToRemove.isPresent())
            statusesToRemove = allStatusesToRemove.get()
                                    .filter(key -> key instanceof DefinedMetricStatuskey)
                                    .map(key -> (DefinedMetricStatuskey) key);
        
		StatusStream<DefinedMetricStatuskey, Metric, VariableStatuses, Metric> statuses = 
				metrics.mapWithState(
				        DefinedMetricStatuskey.class, 
				        VariableStatuses.class, 
				        new ComputeDefinedMetricKeysF(propertiesSourceProps), 
				        Optional.ofNullable(statusesToRemove),
				        new UpdateDefinedMetricStatusesF(propertiesSourceProps));
		
        Stream<Metric> definedMetricsWhenBatch = statuses.getStatuses().transform((rdd, time) -> rdd.flatMap(new ComputeBatchDefineMetricsF(time, propertiesSourceProps)));
        
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
