package ch.cern.spark.status.storage.manager;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.StateImpl;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.metrics.monitors.MonitorStatusKey;
import ch.cern.spark.metrics.notificator.NotificatorStatusKey;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.TestStatus;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import ch.cern.spark.status.storage.StatusesStorage;

public class StatusesManagerCLITest {
    
    private transient KafkaTestUtils kafkaTestUtils;
    private String topic;
    private StatusesManagerCLI manager;
    private KafkaProducer<String, String> producer;
    private JSONStatusSerializer serializer;
    private Properties properties;
    
    @Before
    public void setUp() throws Exception {
        kafkaTestUtils = new KafkaTestUtils();
        kafkaTestUtils.setup();

        topic = "test";
        kafkaTestUtils.createTopic(topic);
        
        manager = new StatusesManagerCLI();
        
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", kafkaTestUtils.brokerAddress());
        configs.put("key.serializer", StringSerializer.class);
        configs.put("value.serializer", StringSerializer.class);
        producer = new KafkaProducer<>(configs);
        
        properties = new Properties();
        properties.setProperty(StatusesStorage.STATUS_STORAGE_PARAM + ".type", "kafka");
        properties.setProperty(StatusesStorage.STATUS_STORAGE_PARAM + ".topic", topic);
        properties.setProperty(StatusesStorage.STATUS_STORAGE_PARAM + ".consumer.bootstrap.servers", kafkaTestUtils.brokerAddress());
        properties.setProperty(StatusesStorage.STATUS_STORAGE_PARAM + ".producer.bootstrap.servers", kafkaTestUtils.brokerAddress());
        properties.setProperty("", kafkaTestUtils.brokerAddress());
        
        serializer = new JSONStatusSerializer();
    }
    
    @Test
    public void emptyTopic() throws ConfigurationException, IOException {
        cmd("-conf /path/");
        
        assertEquals(0, manager.loadAndFilter().count());
    }
    
    @Test
    public void returnAll() throws ConfigurationException, IOException {
        cmd("-conf /path/");
        
        sendMessage(new DefinedMetricStatuskey("dm1", new HashMap<>()), new TestStatus(1));
        sendMessage(new DefinedMetricStatuskey("dm2", new HashMap<>()), new TestStatus(1));
        sendMessage(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1));
        sendMessage(new MonitorStatusKey("m2", new HashMap<>()), new TestStatus(1));
        sendMessage(new NotificatorStatusKey("m1", "n1", new HashMap<>()), new TestStatus(1));
        sendMessage(new NotificatorStatusKey("m1", "n2", new HashMap<>()), new TestStatus(1));
        
        assertEquals(6, manager.loadAndFilter().count());
    }
    
    @Test
    public void filterByClassName() throws ConfigurationException, IOException {
        cmd("-conf /path/ -fqcn ch.cern.spark.metrics.defined.DefinedMetricStatuskey");
        
        sendMessage(new DefinedMetricStatuskey("dm1", new HashMap<>()), new TestStatus(1));
        sendMessage(new DefinedMetricStatuskey("dm2", new HashMap<>()), new TestStatus(1));
        
        sendMessage(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1));
        sendMessage(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1));
        sendMessage(new MonitorStatusKey("m2", new HashMap<>()), new TestStatus(1));
        sendMessage(new NotificatorStatusKey("m1", "n1", new HashMap<>()), new TestStatus(1));
        sendMessage(new NotificatorStatusKey("m1", "n2", new HashMap<>()), new TestStatus(1));
        
        assertEquals(2, manager.loadAndFilter().count());
    }
    
    @Test
    public void filterByAliasClassName() throws ConfigurationException, IOException {
        cmd("-conf /path/ -fqcn defined-metric-key");
        
        sendMessage(new DefinedMetricStatuskey("dm1", new HashMap<>()), new TestStatus(1));
        sendMessage(new DefinedMetricStatuskey("dm2", new HashMap<>()), new TestStatus(1));
        
        sendMessage(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1));
        sendMessage(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1));
        sendMessage(new MonitorStatusKey("m2", new HashMap<>()), new TestStatus(1));
        sendMessage(new NotificatorStatusKey("m1", "n1", new HashMap<>()), new TestStatus(1));
        sendMessage(new NotificatorStatusKey("m1", "n2", new HashMap<>()), new TestStatus(1));
        
        assertEquals(2, manager.loadAndFilter().count());
    }
    
    @Test
    public void filterByID() throws ConfigurationException, IOException {
        cmd("-conf /path/ -id dm2");
        
        sendMessage(new DefinedMetricStatuskey("dm1", new HashMap<>()), new TestStatus(1));
        sendMessage(new DefinedMetricStatuskey("dm2", new HashMap<>()), new TestStatus(1));
        
        sendMessage(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1));
        sendMessage(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1));
        sendMessage(new MonitorStatusKey("m2", new HashMap<>()), new TestStatus(1));
        sendMessage(new NotificatorStatusKey("m1", "n1", new HashMap<>()), new TestStatus(1));
        sendMessage(new NotificatorStatusKey("m1", "n2", new HashMap<>()), new TestStatus(1));
        
        assertEquals(1, manager.loadAndFilter().count());
    }
    
    @Test
    public void filterByExpiration() throws ConfigurationException, IOException {
        cmd("-conf /path/ -expired 4m");
        
        Instant now = Instant.now();
        
        //Time = 0
        sendMessage(new DefinedMetricStatuskey("dm1", new HashMap<>()), new TestStatus(1));
        
        StatusValue status = new TestStatus(1);
        status.update(new StateImpl<>(), new Time(now.minus(Duration.ofMinutes(10)).toEpochMilli()));
        sendMessage(new DefinedMetricStatuskey("dm2", new HashMap<>()), status);
        
        status = new TestStatus(1);
        status.update(new StateImpl<>(), new Time(now.minus(Duration.ofMinutes(5)).toEpochMilli()));
        sendMessage(new DefinedMetricStatuskey("dm3", new HashMap<>()), status);
        
        status = new TestStatus(1);
        status.update(new StateImpl<>(), new Time(now.minus(Duration.ofMinutes(3)).toEpochMilli()));
        sendMessage(new DefinedMetricStatuskey("dm4", new HashMap<>()), status);
        
        status = new TestStatus(1);
        status.update(new StateImpl<>(), new Time(now.minus(Duration.ofMinutes(1)).toEpochMilli()));
        sendMessage(new DefinedMetricStatuskey("dm5", new HashMap<>()), status);
        
        assertEquals(3, manager.loadAndFilter().count());
    }

    private void cmd(String cmdString) throws ConfigurationException {
        String[] args = (cmdString).split(" ");
        CommandLine cmd = StatusesManagerCLI.parseCommand(args);
        
        manager.config(properties, cmd);
    }

    private void sendMessage(StatusKey key, StatusValue value) throws IOException {
        String keyS = key != null ? new String(serializer.fromKey(key)) : null;
        String valueS = value != null ? new String(serializer.fromValue(value)) : null;
        
        producer.send(new ProducerRecord<String, String>(topic, keyS, valueS));
        producer.flush();
    }
    
    @After
    public void shutDown() {
        manager.close();
    }

}
