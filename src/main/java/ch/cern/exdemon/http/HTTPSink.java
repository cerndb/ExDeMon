package ch.cern.exdemon.http;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

import ch.cern.Taggable;
import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.json.JSON;
import ch.cern.exdemon.json.JSONParser;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.action.Template;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.utils.TimeUtils;
import lombok.ToString;

@ToString(callSuper=false)
public class HTTPSink implements Serializable{
	
	private static final long serialVersionUID = 2779022310649799825L;

	private final static Logger LOG = LogManager.getLogger(HTTPSink.class);

	private static HttpClient httpClient;

	public static final String URL_PARAM = "url";
	private String url;
	
	public static final String RETRIES_PARAM = "retries";
	private static HttpRequestRetryHandler retryHandler;
	
	public static final String TIMEOUT_PARAM = "timeout";
	private int timeout_ms;
	
	public static final String PARALLELIZATION_PARAM = "parallelization";
	private int parallelization;
	
	public static final String BATCH_SIZE_PARAM = "batch.size";
	private int batch_size;
	
	public static final String AUTH_PARAM = "auth";
	public static final String AUTH_TYPE_PARAM = AUTH_PARAM + ".type";
    public static final String AUTH_USERNAME_PARAM = AUTH_PARAM + ".user";
    public static final String AUTH_PASSWORD_PARAM = AUTH_PARAM + ".password";
    private UsernamePasswordCredentials authCredentials;
    
	private Map<String, String> propertiesToAdd;
	
	private static final String AS_ARRAY_PARAM = "as-array";
    private boolean as_array;

    private boolean addAction;

	public ConfigurationResult config(Properties properties) {
	    ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
	    
		url = properties.getProperty(URL_PARAM);
		if(url == null)
		    confResult.withMustBeConfigured(URL_PARAM);
		
		int retries = (int) properties.getFloat(RETRIES_PARAM, 1);
		retryHandler = new StandardHttpRequestRetryHandler(retries, false);
		
		timeout_ms = (int) properties.getFloat(TIMEOUT_PARAM, 5000);
		parallelization = (int) properties.getFloat(PARALLELIZATION_PARAM, 1);
		batch_size = (int) properties.getFloat(BATCH_SIZE_PARAM, 100);
		try {
            as_array = properties.getBoolean(AS_ARRAY_PARAM, true);
        } catch (ConfigurationException e) {
            confResult.withError(null, e);
        }
		
		try {
            addAction = properties.getBoolean("add.$action", true);
        } catch (ConfigurationException e) {
            confResult.withError(null, e);
        }
		
		propertiesToAdd = properties.getSubset("add").toStringMap();
		propertiesToAdd.remove("$action");
		
		String authenticationType = properties.getProperty(AUTH_TYPE_PARAM, "disabled");
        if(authenticationType.equals("basic-user-password")){
            String username = properties.getProperty(AUTH_USERNAME_PARAM);
            String password = properties.getProperty(AUTH_PASSWORD_PARAM);
            
            authCredentials = new UsernamePasswordCredentials(username, password);
        }else if(authenticationType.equals("disabled")){
            authCredentials = null;
        }else {
            confResult.withError(AUTH_TYPE_PARAM, "authentication type \"" + authenticationType + "\" is not available");
        }
        
        return confResult;
	}
	
	public void sink(JavaDStream<?> outputStream) {
		outputStream = outputStream.repartition(parallelization);
		
		JavaDStream<JsonPOSTRequest> requestsStream = outputStream.flatMap(object -> {
		        try {
		            return Collections.singleton(toJsonPOSTRequest(object)).iterator();
		        }catch(Exception e) {
		            LOG.error("Error when parsing object to request. Object=" + String.valueOf(object), e);
		            
		            return Collections.emptyIterator();
		        }
		    });
		
		requestsStream.foreachRDD(rdd -> rdd.foreachPartitionAsync(requests -> batchAndSend(requests)));
	}
	
    public void sink(Object object) throws ParseException {
        JsonPOSTRequest request = toJsonPOSTRequest(object);
        
        batchAndSend(Collections.singleton(request).iterator());
    }

    public JsonPOSTRequest toJsonPOSTRequest(Object object) throws ParseException {
        String url = this.url;
        JSON json = null;
        
        if(object instanceof Action) {
            Action action = (Action) object;            
            url = Template.apply(url, action);
            json = addAction ? JSONParser.parse(object) : new JSON("{}");
        }else if(object instanceof String) {
            json = new JSON((String) object);
        }else {
            json = JSONParser.parse(object);
        }
        
        JsonPOSTRequest request = new JsonPOSTRequest(url, json);
        
        Map<String, String> tags = new HashMap<>();
        if(object instanceof Taggable)
            tags = ((Taggable) object).getTags();
        
        for (Map.Entry<String, String> propertyToAdd : propertiesToAdd.entrySet()) {
            String value = propertyToAdd.getValue();
            
            if(value.startsWith("%"))
                if(tags != null)
                    value = tags.get(value.substring(1));
                else
                    value = null;
            
            if(value != null && object instanceof Action)
                value = Template.apply(value, (Action) object);
            
            if(value != null && value.equals("null"))
                value = null;
                
            if(value != null 
                    && value.startsWith("[")
                    && value.endsWith("]")) {
                String arrayContent = value.substring(1, value.length() - 1);
                String[] arrayContentParts = arrayContent.split("\\+\\+");
                
                JsonArray jsonArray = new JsonArray();
                
                String[] jsonKeys = request.getJson().getAllKeys();
                
                for (String arrayContentPart : arrayContentParts) {
                    if(arrayContentPart.startsWith("keys:")) {
                        String keysRegex = arrayContentPart.replace("keys:", "");
                        
                        Pattern pattern = Pattern.compile(keysRegex);
                        
                        String[] matchingKeys = Arrays.stream(jsonKeys).filter(jsonKey -> pattern.matcher(jsonKey).matches()).toArray(String[]::new);
                        
                        for (String matchingKey : matchingKeys)
                            jsonArray.add(new JsonPrimitive(matchingKey));
                    }
                    
                    if(arrayContentPart.startsWith("attributes:#")) {
                        String tag = arrayContentPart.replace("attributes:#", "");
                        
                        if(tags.containsKey(tag)) {
                            String[] attributesKeys = tags.get(tag).split("\\s");
                            
                            for (String attributeKey : attributesKeys) {
                                String fullKey = "analyzed_metric.attributes." + attributeKey;
                                
                                boolean exist = Arrays.stream(jsonKeys).filter(jsonKey -> jsonKey.equals(fullKey)).count() > 0;
                                
                                if(exist)
                                    jsonArray.add(new JsonPrimitive(fullKey));
                            }
                        }
                    }
                }
                
                request.getJson().getElement().getAsJsonObject().add(propertyToAdd.getKey(), jsonArray);
            }else{
                request.addProperty(propertyToAdd.getKey(), value);
            }
        }
        
        return request;
    }
    
    protected void batchAndSend(Iterator<JsonPOSTRequest> requests) {
        List<JsonPOSTRequest> requestsToSend = new LinkedList<>();
        while (requests.hasNext()) {
            requestsToSend.add(requests.next());
            
            if(requestsToSend.size() >= batch_size) {
                buildBatchAndSend(requestsToSend);
                
                requestsToSend = new LinkedList<>();
            }
        }
        
        buildBatchAndSend(requestsToSend);
    }

    public void buildBatchAndSend(List<JsonPOSTRequest> requests) {
	    if(as_array)
	        requests = buildJSONArrays(requests);

        for (JsonPOSTRequest request : requests)
            trySend(request);
    }

    private static HttpClient getHTTPClient() {
        if(HTTPSink.httpClient == null)
            HTTPSink.httpClient = HttpClients.custom()
                                             .setRetryHandler(retryHandler)
                                             .build();
        
		return HTTPSink.httpClient;
	}
	
	public static HttpClient setHTTPClient(HttpClient httpClient) {
		return HTTPSink.httpClient = httpClient;
	}

	public void trySend(JsonPOSTRequest request) {		
		HttpClient httpClient = getHTTPClient();
		
        try {
            send(httpClient, request);
        } catch (Exception e) {
            LOG.error("Error sending request: " + request, e);
        }
	}
	
	private void send(HttpClient httpClient, JsonPOSTRequest request) throws HttpException, IOException {
        HttpPost postMethod = request.toPostMethod();
        
        if(authCredentials != null)
            postMethod.addHeader(new BasicScheme().authenticate(authCredentials, postMethod, null));
        
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (postMethod != null)
                		postMethod.abort();
            }
        };
        new Timer(true).schedule(task, timeout_ms);
        
		HttpResponse response = null;
        try {
            response = httpClient.execute(postMethod);
        } catch (IOException e) {
            try{
                postMethod.releaseConnection();
                postMethod.abort();
                postMethod.completed();
            }catch(Exception ee){}
            
            throw new HttpException("Unable to POST to url=" + request.getUrl(), e);
        }
		
		if(postMethod.isAborted())
			throw new HttpException("Request has timmed out after " + TimeUtils.toString(Duration.ofMillis(timeout_ms)));
		
		int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 201 || statusCode == 200) {
            LOG.trace("JSON: " + request.getJson() + " sent to " + request.getUrl());
        } else {
            throw new HttpException("Unable to POST to url=" + request.getUrl() + " with status code=" + statusCode + " "+response.toString()+". JSON: " + request.getJson());
        }
	}

    private List<JsonPOSTRequest> buildJSONArrays(List<JsonPOSTRequest> elements) {
	    Map<String, List<JsonPOSTRequest>> groupedByUrl = elements.stream().collect(Collectors.groupingBy(JsonPOSTRequest::getUrl));
	    
	    return groupedByUrl.entrySet().stream().map(entry -> {
            	        String jsonString = entry.getValue().stream().map(req -> req.getJson().toString()).collect(Collectors.toList()).toString();
            	        
            	        return new JsonPOSTRequest(entry.getKey(), new JSON(jsonString));
            	    }).collect(Collectors.toList());
	}

}
