package ch.cern.spark.status.storage.types;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Time;

import com.google.common.collect.Sets;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.ByteArray;
import ch.cern.spark.RDD;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import ch.cern.spark.status.storage.JavaStatusSerializer;
import ch.cern.spark.status.storage.StatusSerializer;
import ch.cern.spark.status.storage.StatusesStorage;
import scala.Tuple2;

@RegisterComponent("kafka")
public class KafkaStatusesStorage extends StatusesStorage {

	private static final long serialVersionUID = 1194347587683707148L;
	
	private transient final static Logger LOG = Logger.getLogger(KafkaStatusesStorage.class.getName());
	
	private Map<String, Object> kafkaProducer = null;
	private Map<String, Object> kafkaConsumer;

	private String topic;
	
	private StatusSerializer serializer;

	private transient KafkaConsumer<Bytes, Bytes> consumer;

    private Duration timeout;
	
	public void config(Properties properties) throws ConfigurationException {
		kafkaProducer = getKafkaProducerParams(properties);
		kafkaConsumer = getKafkaConsumerParams(properties);
        
		topic = properties.getProperty("topic");
		
		timeout = properties.getPeriod("timeout", Duration.ofSeconds(2));
		
		String serializationType = properties.getProperty("serialization", "json");
		switch (serializationType) {
		case "json":
			serializer = new JSONStatusSerializer();
			break;
		case "java":
			serializer = new JavaStatusSerializer();
			break;
		default:
			throw new ConfigurationException("Serialization type " + serializationType + " is not available.");
		}
        
		properties.confirmAllPropertiesUsed();
	}
	
	@Override
	public JavaRDD<Tuple2<StatusKey, StatusValue>> load(JavaSparkContext context) throws IOException, ConfigurationException {
	    List<ConsumerRecordSer> list = getAllRecords();
		
        JavaRDD<ConsumerRecordSer> kafkaContent = context.parallelize(list);
		
		JavaRDD<Tuple2<ByteArray, ByteArray>> latestRecords = getLatestRecords(kafkaContent);
		
		JavaRDD<Tuple2<StatusKey, StatusValue>> parsed = parseRecords(latestRecords);
		
		LOG.info("Statuses loaded from Kafka topic " + topic);
		
		parsed = parsed.persist(StorageLevel.MEMORY_AND_DISK());
		
        return parsed;
	}

    private void setUpConsumer() {
        kafkaConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        
        consumer = new KafkaConsumer<Bytes, Bytes>(kafkaConsumer);
        consumer.subscribe(Sets.newHashSet(topic));
    }

    private List<ConsumerRecordSer> getAllRecords() {
        setUpConsumer();
        
        List<ConsumerRecordSer> list = new LinkedList<>();
        
        long[] lastOffsets = new long[consumer.partitionsFor(topic).size()];
        
        ConsumerRecords<Bytes, Bytes> records = consumer.poll(timeout.toMillis());
        while(!records.isEmpty()) {
            records.forEach(r -> {
                long offset = r.offset();
                if(lastOffsets[r.partition()] < offset)
                    lastOffsets[r.partition()] = offset;
                
                list.add(new ConsumerRecordSer(r));
            });

            records = consumer.poll(timeout.toMillis());
        }
        
        checkAllRecordsConsumed(lastOffsets);
        
        consumer.close();
        
        return list;
    }
    
    public void checkAllRecordsConsumed(long[] lastOffsets) {
        List<TopicPartition> partitions = new LinkedList<>();
        for (PartitionInfo partitionInfo : consumer.partitionsFor(topic))
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));

        long[] until = new long[partitions.size()];

        consumer.seekToEnd(partitions);
        consumer.poll(0);
        for (TopicPartition tp : partitions) {
            until[tp.partition()] = consumer.position(tp) - 1;
            
            if(until[tp.partition()] < 0)
                until[tp.partition()] = 0;
        }
        
        if(!Arrays.equals(until, lastOffsets)) {
            LOG.error("Some partitions were not completelly consumed when reading the state.");
            LOG.error("Topic " + topic + " last partition offsets: " + Arrays.toString(until));
            LOG.error("Consumed from topic " + topic + " till offsets: " + Arrays.toString(lastOffsets));
            
            throw new RuntimeException("Topic has not been completelly consumed.");
        }
    }

    private JavaRDD<Tuple2<ByteArray, ByteArray>> getLatestRecords(JavaRDD<ConsumerRecordSer> kafkaContent) {
		return kafkaContent.mapToPair(consumedRecord -> {
									Tuple2<Long, ByteArray> value = new Tuple2<>(consumedRecord.offset(), consumedRecord.value());
									
									return new Tuple2<ByteArray, Tuple2<Long, ByteArray>>(consumedRecord.key(), value);
								})
								.groupByKey()
								.map(pair -> {
									ByteArray key = pair._1;
									
									Iterator<Tuple2<Long, ByteArray>> values = pair._2.iterator();
									
									Tuple2<Long, ByteArray> latestValue = values.next();
									
									while(values.hasNext()) {
										Tuple2<Long, ByteArray> value = values.next();
										
										if(value._1 > latestValue._1)
											latestValue = value;
									}
									
									return new Tuple2<ByteArray, ByteArray>(key, latestValue._2);
								})
								.filter(pair -> pair._2 != null);
	}
	
	private JavaRDD<Tuple2<StatusKey, StatusValue>> parseRecords(JavaRDD<Tuple2<ByteArray, ByteArray>> latestRecords) {
		return latestRecords.map(binaryRecord -> new Tuple2<>(
														serializer.toKey(binaryRecord._1.get()),
														serializer.toValue(binaryRecord._2.get()))
													);
	}
	
	@Override
	public <K extends StatusKey, V extends StatusValue> void save(RDD<Tuple2<K, V>> rdd, Time time)
			throws IllegalArgumentException, IOException, ConfigurationException {
		
		rdd = filterOnlyUpdatedStates(rdd, time);
		
		rdd.asJavaRDD().foreachPartition(new KafkaProducerFunc<K, V>(kafkaProducer, serializer, topic));
	}
	
    @Override
    public <K extends StatusKey> void remove(RDD<K> rdd) {
        JavaRDD<Tuple2<K, StatusValue>> keyWithNulls = rdd.asJavaRDD().map(key -> new Tuple2<K, StatusValue>(key, null));
        
        keyWithNulls.foreachPartition(new KafkaProducerFunc<K, StatusValue>(kafkaProducer, serializer, topic));
    }
	
    private <K extends StatusKey, V extends StatusValue> RDD<Tuple2<K, V>> filterOnlyUpdatedStates(RDD<Tuple2<K, V>> rdd, Time time) {
		return RDD.from(rdd.asJavaRDD().filter(tuple -> tuple._2 == null || tuple._2.getUpdatedTime() == time.milliseconds() || tuple._2.getUpdatedTime() == 0));
	}

	private Map<String, Object> getKafkaProducerParams(Properties props) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        
        kafkaParams.put("key.serializer", BytesSerializer.class);
        kafkaParams.put("value.serializer", BytesSerializer.class);
        
        Properties kafkaPropertiesFromConf = props.getSubset("producer");
        for (Entry<Object, Object> kafkaPropertyFromConf : kafkaPropertiesFromConf.entrySet()) {
            String key = (String) kafkaPropertyFromConf.getKey();
            String value = (String) kafkaPropertyFromConf.getValue();
            
            kafkaParams.put(key, value);
        }
        
        return kafkaParams;
    }
	
    private Map<String, Object> getKafkaConsumerParams(Properties props) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        
        kafkaParams.put("key.deserializer", BytesDeserializer.class);
        kafkaParams.put("value.deserializer", BytesDeserializer.class);
       
        kafkaParams.put(ConsumerConfig.CLIENT_ID_CONFIG, "spark-metrics-monitor");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        Properties kafkaPropertiesFromConf = props.getSubset("consumer");
        for (Entry<Object, Object> kafkaPropertyFromConf : kafkaPropertiesFromConf.entrySet()) {
            String key = (String) kafkaPropertyFromConf.getKey();
            String value = (String) kafkaPropertyFromConf.getValue();
            
            kafkaParams.put(key, value);
        }
        
        return kafkaParams;
    }
    
    private static class KafkaProducerFunc<K extends StatusKey, V extends StatusValue> implements VoidFunction<Iterator<Tuple2<K, V>>>{

		private static final long serialVersionUID = 3712180876662835316L;
		
		private Map<String, Object> props;

		private String topic;

		private StatusSerializer serializer;
		
		protected KafkaProducerFunc(Map<String, Object> props, StatusSerializer serializer, String topic) {
			this.props = props;
			this.topic = topic;
			this.serializer = serializer;
		}

		@Override
		public void call(Iterator<Tuple2<K, V>> records) throws Exception {
			KafkaProducer<Bytes, Bytes> producer = new KafkaProducer<>(props);
			
			while(records.hasNext()) {
				Tuple2<K, V> record = records.next();

				Bytes key = record._1 != null ? new Bytes(serializer.fromKey(record._1)) : null;
				Bytes value = record._2 != null ? new Bytes(serializer.fromValue(record._2)) : null;
				
				producer.send(new ProducerRecord<Bytes, Bytes>(topic, key, value));
			}
			
			producer.close();
		}
    	
    }

}
