package ch.cern.spark.status.storage.types.kafka;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.spark.metrics.monitors.MonitorStatusKey;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.TestStatus;
import ch.cern.spark.status.storage.JSONStatusSerializer;

public class KafkaStatusesManagementTest {
    
    private transient KafkaTestUtils kafkaTestUtils;
    private String topic;
    private KafkaStatusesManagement manager;
    private KafkaProducer<String, String> producer;
    private JSONStatusSerializer serializer;
    
    @Before
    public void setUp() throws Exception {
        kafkaTestUtils = new KafkaTestUtils();
        kafkaTestUtils.setup();

        topic = "test";
        kafkaTestUtils.createTopic(topic);
        
        manager = new KafkaStatusesManagement();
        
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", kafkaTestUtils.brokerAddress());
        configs.put("key.serializer", StringSerializer.class);
        configs.put("value.serializer", StringSerializer.class);
        producer = new KafkaProducer<>(configs);
        
        serializer = new JSONStatusSerializer();
    }
    
    @Test
    public void emptyTopic() throws ConfigurationException, IOException {
        String[] args = ("-brokers "+kafkaTestUtils.brokerAddress()+" "
                       + "-topic " + topic + " ").split(" ");
        manager.config(args);
        
        assertEquals(0, manager.load().count());
    }
    
    @Test
    public void returnAll() throws ConfigurationException, IOException {
        String[] args = ("-brokers "+kafkaTestUtils.brokerAddress()+" "
                       + "-topic " + topic + " ").split(" ");
        manager.config(args);
        
        sendMessage(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1));
        
        assertEquals(0, manager.load().count());
    }

    private void sendMessage(StatusKey key, StatusValue value) throws IOException {
        String keyS = new String(serializer.fromKey(key));
        String valueS = new String(serializer.fromValue(value));
        
        producer.send(new ProducerRecord<String, String>(topic, keyS, valueS));
    }

}
