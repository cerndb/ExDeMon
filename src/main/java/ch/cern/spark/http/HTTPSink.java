package ch.cern.spark.http;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.Taggable;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.Stream;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.json.JSONParser;
import ch.cern.utils.TimeUtils;

public class HTTPSink implements Serializable{
	
	private static final long serialVersionUID = 2779022310649799825L;

	private final static Logger LOG = LogManager.getLogger(HTTPSink.class);

	public static final String URL_PARAM = "url";

	private static HttpClient httpClient;

	private String url;
	
	public static final String RETRIES_PARAM = "retries";
	private int retries;
	
	public static final String TIMEOUT_PARAM = "timeout";
	private int timeout_ms;
	
	public static final String PARALLELIZATION_PARAM = "parallelization";
	private int parallelization;
	
	public static final String BATCH_SIZE_PARAM = "batch.size";
	private int batch_size;
	
	public static final String AUTH_PARAM = "auth";
    public static final String AUTH_USERNAME_PARAM = AUTH_PARAM + ".user";
    public static final String AUTH_PASSWORD_PARAM = AUTH_PARAM + ".password";
    private Header authHeader;
	
	private Map<String, String> propertiesToAdd;

	public void config(Properties properties) throws ConfigurationException {
		url = properties.getProperty(URL_PARAM);
		retries = (int) properties.getFloat(RETRIES_PARAM, 1);
		timeout_ms = (int) properties.getFloat(TIMEOUT_PARAM, 2000);
		parallelization = (int) properties.getFloat(PARALLELIZATION_PARAM, 1);
		batch_size = (int) properties.getFloat(BATCH_SIZE_PARAM, 100);
		
		propertiesToAdd = properties.getSubset("add").toStringMap();
		
		// Authentication configs
        boolean authentication = properties.getBoolean(AUTH_PARAM);
        if(authentication){
            String username = properties.getProperty(AUTH_USERNAME_PARAM);
            String password = properties.getProperty(AUTH_PASSWORD_PARAM);
            
            try {
                String encoding = Base64.getEncoder().encodeToString((username+":"+password).getBytes("UTF-8"));
                
                authHeader = new Header("Authorization", "Basic " + encoding);
                
                LOG.info("Authentication enabled, user: " + username);
            } catch (UnsupportedEncodingException e) {
                throw new ConfigurationException("Problem when creating authentication header");
            }
        }
	}
	
	public void sink(Stream<?> outputStream) {
		outputStream = outputStream.repartition(parallelization);
		
		Stream<Object> jsonStream = outputStream.map(object -> {
			JSONObject json = JSONParser.parse(object);
			
			Map<String, String> tags = null;
			if(object instanceof Taggable)
				tags = ((Taggable) object).getTags();
			
			for (Map.Entry<String, String> propertyToAdd : propertiesToAdd.entrySet()) {
				String value = propertyToAdd.getValue();
				
				if(value.startsWith("%") && tags != null && tags.containsKey(value.substring(1)))
					value = tags.get(value.substring(1));
				
				json.setProperty(propertyToAdd.getKey(), value);
			}
			
			return json;
		});
		
		JavaDStream<String> jsonStringStream = jsonStream.asString().asJavaDStream();
		
		jsonStringStream.foreachRDD(rdd -> {
			rdd.foreachPartition(strings -> {
				List<Exception> thrownExceptions = new LinkedList<>();
				Exception thrownException = null;
				
				List<String> elementsToSend = new LinkedList<>();
				while (strings.hasNext()) {
					elementsToSend.add((String) strings.next());
					
					if(elementsToSend.size() >= batch_size) {
						thrownException = sendBatch(elementsToSend);
						if(thrownException != null)
							thrownExceptions.add(thrownException);
						
						elementsToSend = new LinkedList<>();
					}
				}
				
				thrownException = sendBatch(elementsToSend);
				if(thrownException != null)
					thrownExceptions.add(thrownException);
				
				if(!thrownExceptions.isEmpty())
				    LOG.error(new IOException("Same batches could not be sent. Exceptions: " + thrownExceptions));
			});
		});
	}

	private static HttpClient getHTTPClient() {
		return HTTPSink.httpClient == null ? new HttpClient() : HTTPSink.httpClient;
	}
	
	public static HttpClient setHTTPClient(HttpClient httpClient) {
		return HTTPSink.httpClient = httpClient;
	}

	private Exception sendBatch(List<String> elementsToSend) {
		if(elementsToSend.size() <= 0)
			return null;
		
		HttpClient httpClient = getHTTPClient();
		
		Exception thrownException = new Exception();
		
		for (int i = 0; i < retries && thrownException != null; i++) {
			try {
				trySendBatch(httpClient, elementsToSend);
				
				thrownException = null;
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				
				thrownException = e;
				
				setHTTPClient(null);
				httpClient = getHTTPClient();
			}	
		}
		
		return thrownException;	
	}
	
	private void trySendBatch(HttpClient httpClient, List<String> elementsToSend) throws HttpException, IOException {
        StringRequestEntity requestEntity = new StringRequestEntity(buildJSONArray(elementsToSend), "application/json", "UTF-8");
        PostMethod postMethod = new PostMethod(url);
        postMethod.setRequestEntity(requestEntity);
        
        if(authHeader != null)
            postMethod.setRequestHeader(authHeader);
        
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (postMethod != null)
                		postMethod.abort();
            }
        };
        new Timer(true).schedule(task, timeout_ms);
        
		int statusCode = httpClient.executeMethod(postMethod);
		
		if(postMethod.isAborted())
			throw new HttpException("Request has timmed out after " + TimeUtils.toString(Duration.ofMillis(timeout_ms)));
		
        if (statusCode != 201 && statusCode != 200) {
        		throw new HttpException("Unable to POST to url=" + url + " with status code=" + statusCode);
        } else {
            LOG.info("Batch of " + elementsToSend.size() + " JSON documents sent to " + url);
        }
	}

	private String buildJSONArray(List<String> elements) {
		return elements.toString();
	}

}
