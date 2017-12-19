package ch.cern.spark.status;

import java.io.IOException;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.ActionOrValue.Action;
import ch.cern.spark.status.storage.StatusesStorage;
import scala.Option;
import scala.Tuple2;

public class State<K, V> {
	
	public static final String STATUSES_EXPIRATION_PERIOD_PARAM = "spark.cern.streaming.status.timeout";

	public static<K extends StatusKey, V, S extends StatusValue, R> JavaMapWithStateDStream<K, ActionOrValue<V>, S, R> map(
			Class<K> keyClass,
			Class<S> statusClass,
			JavaPairDStream<K, V> valuesStream,
			UpdateStatusFunction<K, V, S, R> updateStatusFunction,
			Optional<JavaDStream<K>> removeKeysStream) 
					throws ClassNotFoundException, IOException, ConfigurationException {
		
		JavaSparkContext context = JavaSparkContext.fromSparkContext(valuesStream.context().sparkContext());
		
		Optional<StatusesStorage> storageOpt = getStorage(context);
		if(!storageOpt.isPresent())
			throw new ConfigurationException("Storage needs to be configured");
		StatusesStorage storage = storageOpt.get();
		
		JavaPairRDD<K, S> initialStates = storage.load(context, keyClass, statusClass);

        StateSpec<K, ActionOrValue<V>, S, R> statusSpec = StateSpec.function(updateStatusFunction).initialState(initialStates.rdd());
        
        Option<Duration> timeout = getStatusExpirationPeriod(context);
        if(timeout.isDefined())
            statusSpec = statusSpec.timeout(timeout.get());
        
        JavaPairDStream<K, ActionOrValue<V>> actionsAndValues = valuesStream.mapToPair(tuple -> new Tuple2<K, ActionOrValue<V>>(tuple._1, new ActionOrValue<>(tuple._2)));

        if(removeKeysStream.isPresent()) {
            actionsAndValues = actionsAndValues.union(
                    removeKeysStream.get().mapToPair(k -> new Tuple2<K, ActionOrValue<V>>(k, new ActionOrValue<>(Action.REMOVE))));
            
            removeKeysStream.get().foreachRDD(rdd -> storage.remove(rdd));
        }
        
        JavaMapWithStateDStream<K, ActionOrValue<V>, S, R> statusStream = actionsAndValues.mapWithState(statusSpec);
        
        statusStream.stateSnapshots().foreachRDD((rdd, time) -> storage.save(rdd, time));
        		
		return statusStream;
	}

    private static Option<Duration> getStatusExpirationPeriod(JavaSparkContext context) {
		SparkConf conf = context.getConf();
		
		Option<String> valueString = conf.getOption(STATUSES_EXPIRATION_PERIOD_PARAM);
		
		if(valueString.isDefined())
		    return Option.apply(new Duration(java.time.Duration.parse(valueString.get()).toMillis()));
		else
		    return Option.empty();
	}

	private static java.util.Optional<StatusesStorage> getStorage(JavaSparkContext context) throws ConfigurationException {
		Properties sparkConf = Properties.from(context.getConf().getAll());
		Properties storageConfig = sparkConf.getSubset(StatusesStorage.STATUS_STORAGE_PARAM);
		
		java.util.Optional<StatusesStorage> storage = ComponentManager.buildOptional(Type.STATUS_STORAGE, storageConfig);
		
		return storage;
	}
	
}
