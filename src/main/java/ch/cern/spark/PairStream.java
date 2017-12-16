package ch.cern.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusStream;
import ch.cern.spark.status.StatusValue;
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

	public static<K extends StatusKey, V, S extends StatusValue, R> StatusStream<K, V, S, R> mapWithState(
			Class<K> keyClass,
			Class<S> statusClass,
			PairStream<K, V> input,
			Function4<Time, K, Optional<V>, State<S>, Optional<R>> updateStatusFunction) 
					throws ClassNotFoundException, IOException, ConfigurationException {
		
		JavaSparkContext context = input.getSparkContext();
		
		java.util.Optional<StatusesStorage> storageOpt = getStorage(context);
		if(!storageOpt.isPresent())
			throw new ConfigurationException("Storgae need to be configured");
		StatusesStorage storage = storageOpt.get();
		
		JavaRDD<Tuple2<K, S>> initialStates = storage.load(context, keyClass, statusClass);

        StateSpec<K, V, S, R> statusSpec = StateSpec
							                .function(updateStatusFunction)
							                .initialState(initialStates.rdd());
        
        Option<Duration> timeout = getStatusExpirationPeriod(input.getSparkContext());
        if(timeout.isDefined())
            statusSpec = statusSpec.timeout(timeout.get());
        
        StatusStream<K, V, S, R> statusStream = StatusStream.from(input.asJavaDStream()
        																	.mapToPair(pair -> pair)
        																	.mapWithState(statusSpec));
        
        statusStream.getStatuses().foreachRDD((rdd, time) -> storage.save(rdd, time));
        		
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
