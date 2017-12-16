package ch.cern.spark;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.properties.ConfigurationException;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.json.JSONParser;
import ch.cern.spark.metrics.Sink;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusStream;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.UpdateStatusFunction;

public class Stream<V> {

	private JavaDStream<V> stream;
	
	protected Stream(JavaDStream<V> stream) {
		this.stream = stream;
	}
	
	public static<V> Stream<V> from(JavaDStream<V> input){
		return new Stream<>(input);
	}
	
	public<K> PairStream<K, V> toPair(PairFlatMapFunction<V, K, V> function) {
		return PairStream.from(stream.flatMapToPair(function));
	}
	
	public<K extends StatusKey, S extends StatusValue, R> StatusStream<K, V, S, R> mapWithState(
			Class<K> keyClass, 
			Class<S> statusClass,
			PairFlatMapFunction<V, K, V> toPairFunction, 
			Optional<Stream<K>> removeKeys,
			UpdateStatusFunction<K, V, S, R> updateStatusFunction) throws ClassNotFoundException, IOException, ConfigurationException {

		PairStream<K, V> keyValuePairs = toPair(toPairFunction);
		
		return PairStream.mapWithState(keyClass, statusClass, keyValuePairs, updateStatusFunction, removeKeys);
	}

	public Stream<V> union(Stream<V> input) {
		return Stream.from(stream.union(input.stream));
	}
	
	public Stream<V> union(List<Stream<V>> list) {
		if(list.size() == 0)
			return this;
		
		Stream<V> streams = null;
			
		for (Stream<V> stream : list)
			if(streams == null)
				streams = stream;
			else
				streams = streams.union(stream);

		return streams;
	}
	
	public<R> Stream<R> map(Function<V, R> mapFunction) {
		return Stream.from(stream.map(mapFunction));
	}
	
	public Stream<V> filter(Function<V, Boolean> filterFunction) {
		return Stream.from(stream.filter(filterFunction));
	}
	
	public void foreachRDD(VoidFunction<RDD<V>> function) {
		stream.foreachRDD(rdd -> function.call(RDD.from(rdd)));
	}
	
	public void foreachRDD(VoidFunction2<RDD<V>, Time> function) {
		stream.foreachRDD((rdd, time) -> function.call(RDD.from(rdd), time));
	}
	
	public<R> Stream<R> transform(Function2<JavaRDD<V>, Time, JavaRDD<R>> transformFunc) {
		return Stream.from(stream.transform(transformFunc));
	}

	public Stream<JSONObject> asJSON() {
		return map(JSONParser::parse);
	}

	public Stream<String> asString() {
		return map(Object::toString);
	}

	public JavaDStream<V> asJavaDStream() {
		return stream;
	}

	public <R> Stream<R> mapS(Function<Stream<V>, Stream<R>> mapStreamFucntion) throws Exception {
		return mapStreamFucntion.call(this);
	}
	
	public void sink(Sink<V> sink) {
		sink.sink(this);
	}
	
	public JavaSparkContext getSparkContext() {
		return JavaSparkContext.fromSparkContext(stream.context().sparkContext());
	}

	public Stream<V> repartition(int parallelization) {
		return Stream.from(stream.repartition(parallelization));
	}

	public<R> Stream<R> flatMap(FlatMapFunction<V, R> func) {
		return Stream.from(stream.flatMap(func));
	}

	public void cache() {
		stream = stream.cache();
	}

    public<K, T> PairStream<K, T> mapToPair(PairFunction<V, K, T> func) {
        return PairStream.from(stream.mapToPair(func));
    }
	
}
