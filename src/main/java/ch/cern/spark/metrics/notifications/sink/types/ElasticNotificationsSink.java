package ch.cern.spark.metrics.notifications.sink.types;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.json.JSONParser;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;

@RegisterComponent("elastic")
public class ElasticNotificationsSink extends NotificationsSink {
    
    private static final long serialVersionUID = 6247567528073485033L;

    private String indexName;

    private Map<String, String> elasticConfig;

    @Override
    public void config(Properties properties) throws ConfigurationException {
        super.config(properties);
        
        indexName = properties.getProperty("index");
        
        elasticConfig = getElasticConfig(properties);
    }
    
    private Map<String, String> getElasticConfig(Properties properties) {
        Properties esProps = properties.getSubset("es");
        
        Map<String, String> outputMap = new HashMap<>();
        
        for (Entry<Object, Object> entry : esProps.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            
            outputMap.put("es." + key, value);
        }
        
        return outputMap;
    }

    @Override
    public void notify(JavaDStream<Notification> outputStream) {
        JavaDStream<String> jsonStringsStream = outputStream.map(JSONParser::parse).map(Object::toString);
        
        JavaEsSparkStreaming.saveJsonToEs(jsonStringsStream, indexName, elasticConfig);
    }

}
