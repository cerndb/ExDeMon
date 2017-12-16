package ch.cern.spark.status.storage.types;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Time;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.RDD;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.StatusesStorage;
import scala.NotImplementedError;
import scala.Tuple2;

@RegisterComponent("single-file")
public class SingleFileStatusesStorage extends StatusesStorage{

	private static final long serialVersionUID = 689960361701601565L;

	private static transient FileSystem fs = null;
	
	private String path;
	
	public SingleFileStatusesStorage() {
	}

	@Override
	public void config(Properties properties) throws ConfigurationException {
		super.config(properties);
		
		path = properties.getProperty("path");
	}
	
	@Override
	public <K extends StatusKey, V extends StatusValue> void save(RDD<Tuple2<K, V>> rdd, Time time)
			throws IllegalArgumentException, IOException, ConfigurationException {
		save(rdd.asJavaRDD().collect());
	}
	
	public <K extends StatusKey, V extends StatusValue> void save(List<Tuple2<K, V>> elements) throws IOException {
		if(elements.size() <= 0)
			return;
		
		Class<? extends StatusKey> type = elements.iterator().next()._1.getClass();
		Path storingPath = getStoringFile(type);
		
		setFileSystem();

		Path tmpFile = storingPath.suffix(".tmp");

		fs.mkdirs(tmpFile.getParent());

		ObjectOutputStream oos = new ObjectOutputStream(fs.create(tmpFile, true));
		oos.writeObject(elements);
		oos.close();

		if (canBeRead(tmpFile)) {
			fs.delete(storingPath, false);
			fs.rename(tmpFile, storingPath);
		}

	}
	
    private boolean canBeRead(Path tmpFile) throws IOException {
        setFileSystem();
        
        try {
            ObjectInputStream is = new ObjectInputStream(fs.open(tmpFile));
            is.readObject();
            is.close();
            
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    @SuppressWarnings("unchecked")
	@Override
    public <K extends StatusKey, V extends StatusValue> JavaRDD<Tuple2<K, V>> load(
    		JavaSparkContext context, Class<K> keyClass, Class<V> valueClass) 
    				throws IOException, ConfigurationException {

    		Path storingPath = getStoringFile(keyClass);
		
    		JavaRDD<Tuple2<StatusKey, StatusValue>> statuses = context.parallelize(load(storingPath));
		
		return statuses.filter(status -> status._1.getClass().isAssignableFrom(keyClass) && status._2.getClass().isAssignableFrom(valueClass))
						.map(status -> new Tuple2<K, V>((K)status._1, (V)status._2));
    }
    
	@Override
	public JavaRDD<Tuple2<StatusKey, StatusValue>> load(JavaSparkContext context)
			throws IOException, ConfigurationException {
		throw new NotImplementedError();
	}

	@SuppressWarnings("unchecked")
	public List<Tuple2<StatusKey, StatusValue>> load(Path storingPath) throws IOException {
		setFileSystem();

		Path tmpFile = storingPath.suffix(".tmp");

		if (!fs.exists(storingPath) && fs.exists(tmpFile))
			fs.rename(tmpFile, storingPath);

		if (!fs.exists(storingPath))
			return new LinkedList<Tuple2<StatusKey, StatusValue>>();

		ObjectInputStream is = new ObjectInputStream(fs.open(storingPath));

		List<Tuple2<StatusKey, StatusValue>> elements = null;
		try {
			elements = (List<Tuple2<StatusKey, StatusValue>>) is.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}

		is.close();

		return elements;
	}

	private Path getStoringFile(Class<? extends StatusKey> type) {
		return new Path(path).suffix("/" + type.getSimpleName()).suffix("/latest");
	}

	private void setFileSystem() throws IOException {
		if (fs == null)
			fs = FileSystem.get(new Configuration());
	}

    @Override
    public <K extends StatusKey> void remove(RDD<K> rdd) {
    }
	
}