package ch.cern.spark.status.storage;

import java.io.IOException;

import org.apache.commons.lang.ClassUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Time;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.properties.ConfigurationException;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import scala.Tuple2;

@ComponentType(Type.STATUS_STORAGE)
public abstract class StatusesStorage extends Component {

    private static final long serialVersionUID = 2311234802969980073L;

    public static final String STATUS_STORAGE_PARAM = "spark.cern.streaming.status.storage";

    public abstract <K extends StatusKey, V extends StatusValue> void save(JavaPairRDD<K, V> rdd, Time time)
            throws IllegalArgumentException, IOException, ConfigurationException;

    @SuppressWarnings("unchecked")
    public <K extends StatusKey, V extends StatusValue> JavaPairRDD<K, V> load(JavaSparkContext context,
            Class<K> keyClass, Class<V> valueClass) throws IOException, ConfigurationException {

        JavaRDD<Tuple2<StatusKey, StatusValue>> statuses = load(context);

        JavaPairRDD<K, V> filtered = statuses
                                    .filter(status -> (keyClass == null || (status._1 != null && ClassUtils.isAssignable(status._1.getClass(), keyClass)))
                                                    && (valueClass == null || (status._2 != null && ClassUtils.isAssignable(status._2.getClass(), valueClass))))
                                    .mapToPair(status -> new Tuple2<K, V>((K) status._1, (V) status._2));

        return filtered;
    }

    public abstract JavaRDD<Tuple2<StatusKey, StatusValue>> load(JavaSparkContext context)
            throws IOException, ConfigurationException;

    public abstract <K extends StatusKey> void remove(JavaRDD<K> rdd);

}