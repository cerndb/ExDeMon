package ch.cern.spark;

import java.io.IOException;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.ActionOrValue;
import ch.cern.spark.status.ActionOrValue.Action;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusStream;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.UpdateStatusFunction;
import ch.cern.spark.status.storage.StatusesStorage;
import scala.Option;
import scala.Tuple2;

public class PairStream<K, V> extends Stream<Tuple2<K, V>>{
	
	public static final String STATUSES_EXPIRATION_PERIOD_PARAM = "spark.cern.streaming.status.timeout";
	
	private PairStream(JavaPairDStream<K, V> stream) {
		super(stream.map(tuple -> tuple));
	}

	public static<K, V> PairStream<K, V> from(JavaPairDStream<K, V> input) {
		return new PairStream<>(input);
	}
	
    public static <K, V> PairStream<K, V> fromT(JavaDStream<Tuple2<K, V>> input) {
        return new PairStream<>(input.mapToPair(p -> p));
    }

	public static<K extends StatusKey, V, S extends StatusValue, R> StatusStream<K, V, S, R> mapWithState(
			Class<K> keyClass,
			Class<S> statusClass,
			PairStream<K, V> values,
			UpdateStatusFunction<K, V, S, R> updateStatusFunction,
			Optional<Stream<K>> removeKeys) 
					throws ClassNotFoundException, IOException, ConfigurationException {
		
		JavaSparkContext context = values.getSparkContext();
		
		java.util.Optional<StatusesStorage> storageOpt = getStorage(context);
		if(!storageOpt.isPresent())
			throw new ConfigurationException("Storgae need to be configured");
		StatusesStorage storage = storageOpt.get();
		
		JavaRDD<Tuple2<K, S>> initialStates = storage.load(context, keyClass, statusClass);

        StateSpec<K, ActionOrValue<V>, S, R> statusSpec = StateSpec
        							                .function(updateStatusFunction)
        							                .initialState(initialStates.rdd());
        
        Option<Duration> timeout = getStatusExpirationPeriod(values.getSparkContext());
        if(timeout.isDefined())
            statusSpec = statusSpec.timeout(timeout.get());
        
        PairStream<K, ActionOrValue<V>> actionsAndValues = values.mapToPair(tuple -> new Tuple2<K, ActionOrValue<V>>(tuple._1, new ActionOrValue<>(tuple._2)));

        if(removeKeys.isPresent())
            actionsAndValues = actionsAndValues.union(
                    removeKeys.get().mapToPair(k -> new Tuple2<K, ActionOrValue<V>>(k, new ActionOrValue<>(Action.REMOVE))));
        
        StatusStream<K, V, S, R> statusStream = StatusStream.from(actionsAndValues.asJavaDStream()
                    																	.mapToPair(pair -> pair)
                    																	.mapWithState(statusSpec));
        
        statusStream.getStatuses().foreachRDD((rdd, time) -> storage.save(rdd, time));
        		
		return statusStream;
	}

	private PairStream<K, V> union(PairStream<K, V> other) {
        return fromT(super.union(Stream.from(asJavaDStream())).asJavaDStream());
    }

    private static Option<Duration> getStatusExpirationPeriod(JavaSparkContext context) {
		SparkConf conf = context.getConf();
		
		Option<String> valueString = conf.getOption(STATUSES_EXPIRATION_PERIOD_PARAM);
		
		if(valueString.isDefined())
		    return Option.apply(new Duration(java.time.Duration.parse(valueString.get()).toMillis()));
		else
		    return Option.empty();
	}

	public JavaPairDStream<K, V> asJavaPairDStream() {
		return asJavaDStream().mapToPair(val -> (Tuple2<K, V>) val);
	}

	private static java.util.Optional<StatusesStorage> getStorage(JavaSparkContext context) throws ConfigurationException {
		Properties sparkConf = Properties.from(context.getConf().getAll());
		Properties storageConfig = sparkConf.getSubset(StatusesStorage.STATUS_STORAGE_PARAM);
		
		java.util.Optional<StatusesStorage> storage = ComponentManager.buildOptional(Type.STATUS_STORAGE, storageConfig);
		
		return storage;
	}
	
}
