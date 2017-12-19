package ch.cern.spark.metrics.results.sink.types;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.http.HTTPSink;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;

@RegisterComponent("http")
public class HTTPAnalysisResultSink extends AnalysisResultsSink {

	private static final long serialVersionUID = 6368509840922047167L;

	private HTTPSink sink = new HTTPSink();

	@Override
	public void config(Properties properties) throws ConfigurationException {
		super.config(properties);
		
		properties.setPropertyIfAbsent(HTTPSink.PARALLELIZATION_PARAM, "5");
		sink.config(properties);
	}
	
	@Override
	public void sink(JavaDStream<AnalysisResult> outputStream) {
		sink.sink(outputStream);
	}

}
