package ch.cern.spark.metrics.notifications.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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

import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.http.HTTPSink;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.sink.types.CERNGNINotificationsSink;

public class CERNGNINotificationsSinkTest extends StreamTestHelper<Notification, Notification>{
	
	private static final long serialVersionUID = -8846451662432392890L;
	
	JsonParser JSON = new JsonParser();

	@Test
	public void send() throws ConfigurationException, HttpException, IOException, ParseException {
		HttpClient httpClient = mock(HttpClient.class);
		when(httpClient.executeMethod(anyObject())).thenReturn(201);
		
		HTTPSink.setHTTPClient(httpClient);
		
        Properties properties = new Properties();
		properties.setProperty("content.header.h1", "v1");
		properties.setProperty("content.body.metadata.bm1", "bm1");
		properties.setProperty("content.body.payload.bp1", "bp1");
        
		Notification notification = new Notification();
		Set<String> sinks = new HashSet<>();
		sinks.add("ALL");
		notification.setSinkIds(sinks);
		addInput(0, notification);
        
		JavaDStream<Notification> resultsStream = createStream(Notification.class);
        
        CERNGNINotificationsSink sink = new CERNGNINotificationsSink();
        sink.config(properties);
        sink.sink(resultsStream);
		
		start();
		
		ArgumentCaptor<PostMethod> methodCaptor = ArgumentCaptor.forClass(PostMethod.class);
		verify(httpClient, times(1)).executeMethod(methodCaptor.capture());
		
		StringRequestEntity receivedEntity = (StringRequestEntity) methodCaptor.getAllValues().get(0).getRequestEntity();
		JSONObject json = new JSONObject(JSON.parse(receivedEntity.getContent()).getAsJsonArray().get(0).getAsJsonObject());
		assertEquals("v1", json.getProperty("header.h1"));
		assertEquals("2", json.getProperty("header.m_version"));
		assertEquals("notification", json.getProperty("header.m_type"));
		assertNotNull(json.getProperty("body.metadata.uuid"));
		assertNotNull(json.getProperty("body.metadata.timestamp"));
		assertEquals("bm1", json.getProperty("body.metadata.bm1"));
		assertEquals("bp1", json.getProperty("body.payload.bp1"));
	}
	
	@Test
	public void shouldConvertIntegers() throws ConfigurationException, HttpException, IOException, ParseException {
		HttpClient httpClient = mock(HttpClient.class);
		when(httpClient.executeMethod(anyObject())).thenReturn(201);
		
		HTTPSink.setHTTPClient(httpClient);
		
        Properties properties = new Properties();
		properties.setProperty("content.body.metadata.metric_id", "12");
		properties.setProperty("content.body.metadata.snow_assignment_level", "13");
        
		Notification notification = new Notification();
		Set<String> sinks = new HashSet<>();
		sinks.add("ALL");
		notification.setSinkIds(sinks);
		addInput(0, notification);
        
		JavaDStream<Notification> resultsStream = createStream(Notification.class);
        
        CERNGNINotificationsSink sink = new CERNGNINotificationsSink();
        sink.config(properties);
        sink.sink(resultsStream);
		
		start();
		
		ArgumentCaptor<PostMethod> methodCaptor = ArgumentCaptor.forClass(PostMethod.class);
		verify(httpClient, times(1)).executeMethod(methodCaptor.capture());
		
		StringRequestEntity receivedEntity = (StringRequestEntity) methodCaptor.getAllValues().get(0).getRequestEntity();
		JSONObject json = new JSONObject(JSON.parse(receivedEntity.getContent()).getAsJsonArray().get(0).getAsJsonObject());
		
		assertTrue(json.getElement("body.metadata.timestamp") instanceof JsonPrimitive);
		assertTrue(((JsonPrimitive) json.getElement("body.metadata.timestamp")).getAsInt() > 0);
		
		assertTrue(json.getElement("body.metadata.metric_id") instanceof JsonPrimitive);
		assertEquals(12, ((JsonPrimitive) json.getElement("body.metadata.metric_id")).getAsInt());

		assertTrue(json.getElement("body.metadata.snow_assignment_level") instanceof JsonPrimitive);
		assertEquals(13, ((JsonPrimitive) json.getElement("body.metadata.snow_assignment_level")).getAsInt());
	}

	@Test
	public void shouldExtractValueFromTags() throws ConfigurationException, HttpException, IOException, ParseException {
		HttpClient httpClient = mock(HttpClient.class);
		when(httpClient.executeMethod(anyObject())).thenReturn(201);
		
		HTTPSink.setHTTPClient(httpClient);
		
        Properties properties = new Properties();
		properties.setProperty("content.header.h1", "%header_tag");
		properties.setProperty("content.body.metadata.metric_id", "%metric_id_tag");
		properties.setProperty("content.body.payload.bp1", "%payload_tag");
		properties.setProperty("content.body.payload.bp2", "%no-tag");
        
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
        
        CERNGNINotificationsSink sink = new CERNGNINotificationsSink();
        sink.config(properties);
        sink.sink(resultsStream);
		
		start();
		
		ArgumentCaptor<PostMethod> methodCaptor = ArgumentCaptor.forClass(PostMethod.class);
		verify(httpClient, times(1)).executeMethod(methodCaptor.capture());
		
		StringRequestEntity receivedEntity = (StringRequestEntity) methodCaptor.getAllValues().get(0).getRequestEntity();
		JSONObject json = new JSONObject(JSON.parse(receivedEntity.getContent()).getAsJsonArray().get(0).getAsJsonObject());
		assertEquals("fromtag1", json.getProperty("header.h1"));
		assertEquals("2", json.getProperty("header.m_version"));
		assertEquals("notification", json.getProperty("header.m_type"));
		assertNotNull(json.getProperty("body.metadata.uuid"));
		assertNotNull(json.getProperty("body.metadata.timestamp"));
		assertEquals("fromtag2", json.getProperty("body.payload.bp1"));
		assertEquals("%no-tag", json.getProperty("body.payload.bp2"));
		
		assertTrue(json.getElement("body.metadata.metric_id") instanceof JsonPrimitive);
		assertEquals(1234, ((JsonPrimitive) json.getElement("body.metadata.metric_id")).getAsInt());
	}

}
