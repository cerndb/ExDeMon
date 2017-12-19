package ch.cern.spark.metrics.notifications.sink.types;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.components.RegisterComponent;
import ch.cern.monitoring.gni.GNINotification;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.http.HTTPSink;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;

@RegisterComponent("cern-gni")
public class CERNGNINotificationsSink extends NotificationsSink {

	private static final long serialVersionUID = 6416955181811280312L;
	
	private Properties contentProperties;
	
	private HTTPSink sink = new HTTPSink();

	@Override
	public void config(Properties properties) throws ConfigurationException {
		super.config(properties);
		
		properties.setPropertyIfAbsent(HTTPSink.RETRIES_PARAM, "5");
		sink.config(properties);
		
		contentProperties = properties.getSubset("content");
	}
	
	@Override
	protected void notify(JavaDStream<Notification> notifications) {
	    JavaDStream<GNINotification> gniNotifStream = notifications.map(notification -> {
			return GNINotification.from(contentProperties, notification);
		});

		sink.sink(gniNotifStream);
	}

}
