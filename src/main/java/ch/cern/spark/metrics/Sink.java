package ch.cern.spark.metrics;

import org.apache.spark.streaming.api.java.JavaDStream;

public interface Sink<T> {

	public void sink(JavaDStream<T> outputStream);
	
}
