package ch.cern.spark.metrics.notifications.sink;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.http.HTTPSink;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.sink.types.HTTPNotificationsSink;

public class HTTPNotificationsSinkTest extends StreamTestHelper<Notification, Notification>{
	
	private static final long serialVersionUID = -8846451662432392890L;

	@Test
	public void shouldExtractValueFromTags() throws ConfigurationException, HttpException, IOException, ParseException {
		HttpClient httpClient = mock(HttpClient.class);
		when(httpClient.executeMethod(anyObject())).thenReturn(201);
		
		HTTPSink.setHTTPClient(httpClient);
		
        Properties properties = new Properties();
		properties.setProperty("add.header.h1", "%header_tag");
		properties.setProperty("add.body.metadata.metric_id", "%metric_id_tag");
		properties.setProperty("add.body.payload.bp1", "%payload_tag");
		properties.setProperty("add.body.payload.bp2", "%no-tag");
        
		Notification notification = new Notification();
		Set<String> sinks = new HashSet<>();
		sinks.add("ALL");
		notification.setSinkIds(sinks);
		Map<String, String> tags = new HashMap<>();
		tags.put("header_tag", "fromtag1");
		tags.put("metric_id_tag", "1234");
		tags.put("payload_tag", "fromtag2");
		notification.setTags(tags);
		addInput(0, notification);
        
		JavaDStream<Notification> resultsStream = createStream(Notification.class);
        
        HTTPNotificationsSink sink = new HTTPNotificationsSink();
        sink.config(properties);
        sink.sink(resultsStream);
		
		start();
		
		ArgumentCaptor<PostMethod> methodCaptor = ArgumentCaptor.forClass(PostMethod.class);
		verify(httpClient, times(1)).executeMethod(methodCaptor.capture());
		
		StringRequestEntity receivedEntity = (StringRequestEntity) methodCaptor.getAllValues().get(0).getRequestEntity();
		assertEquals("[{\"tags\":{\"metric_id_tag\":\"1234\",\"payload_tag\":\"fromtag2\",\"header_tag\":\"fromtag1\"},"
					+ "\"sinks\":[\"ALL\"],"
					+ "\"header\":{\"h1\":\"fromtag1\"},"
					+ "\"body\":{"
						+ "\"payload\":{\"bp1\":\"fromtag2\",\"bp2\":\"%no-tag\"},"
						+ "\"metadata\":{\"metric_id\":\"1234\"}}}]", receivedEntity.getContent());
	}

}
