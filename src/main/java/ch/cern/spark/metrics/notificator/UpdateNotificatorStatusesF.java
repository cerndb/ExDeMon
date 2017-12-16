package ch.cern.spark.metrics.notificator;

import java.util.Optional;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.UpdateStatusFunction;

public class UpdateNotificatorStatusesF
        extends UpdateStatusFunction<NotificatorStatusKey, AnalysisResult, StatusValue, Notification> {

    private static final long serialVersionUID = 1540971922358997509L;

    private Properties propertiesSourceProperties;

    public UpdateNotificatorStatusesF(Properties propertiesSourceProps) {
        this.propertiesSourceProperties = propertiesSourceProps;
    }

    @Override
    protected Optional<Notification> update(Time time, NotificatorStatusKey ids, AnalysisResult result, State<StatusValue> status) throws Exception {
        Monitors.initCache(propertiesSourceProperties);

        Optional<Monitor> monitorOpt = Optional.of(Monitors.getCache().get().get(ids.getMonitorID()));
        if (!monitorOpt.isPresent())
            return Optional.empty();
        Monitor monitor = monitorOpt.get();

        Notificator notificator = monitor.getNotificators().get(ids.getNotificatorID());
        if (notificator.hasStatus() && status.exists())
            ((HasStatus) notificator).load(status.get());

        Optional<Notification> notification = notificator.apply(result);

        notificator.getStatus().ifPresent(s -> s.update(status, time));

        notification.ifPresent(n -> {
            n.setMonitorID(ids.getMonitorID());
            n.setNotificatorID(ids.getNotificatorID());
            n.setMetricIDs(ids.getMetricIDs());
            n.setTimestamp(result.getAnalyzedMetric().getInstant());
        });

        return notification;
    }

}
