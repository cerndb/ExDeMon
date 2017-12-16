package ch.cern.spark.status;

import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

import ch.cern.spark.PairStream;
import ch.cern.spark.Stream;

public class StatusStream<K extends StatusKey, V, S extends StatusValue, R> extends Stream<R> {
	
	private PairStream<K, S> statuses;
	
	private StatusStream(JavaMapWithStateDStream<K, ActionOrValue<V>, S, R> stateStream) {
		super(stateStream);
		
		this.statuses = PairStream.from(stateStream.stateSnapshots());
	}

	public static<K extends StatusKey, V, S extends StatusValue, R> StatusStream<K, V, S, R> from(JavaMapWithStateDStream<K, ActionOrValue<V>, S, R> input) {
		return new StatusStream<>(input);
	}

	public PairStream<K, S> getStatuses() {
		return statuses;
	}

}
