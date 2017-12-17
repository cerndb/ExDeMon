package ch.cern.spark.status.storage.manager;

import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.SparkConf;
import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.metrics.monitors.MonitorStatusKey;
import ch.cern.spark.metrics.notificator.NotificatorStatusKey;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.StatusesStorage;
import scala.Tuple2;

public class StatusesManagerCLI {
    
    private StatusesStorage storage;
    private JavaSparkContext context;
    private Class<? extends StatusKey> keyClass;
    
    public StatusesManagerCLI() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("KafkaStatusesManagement");
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.driver.host", "localhost");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        
        context = new JavaSparkContext(sparkConf);
    }
    
    public static void main(String[] args) throws ConfigurationException, IOException {
        StatusesManagerCLI manager = new StatusesManagerCLI();
        CommandLine cmd = parseCommand(args);
        
        Properties properties = Properties.fromFile(cmd.getOptionValue(""));
        
        manager.config(properties, cmd);
        
        JavaRDD<Tuple2<StatusKey, StatusValue>> statuses = manager.load();
        
        statuses.foreach(status -> System.out.println(status));
    }

    public<K extends StatusKey> JavaRDD<Tuple2<K, StatusValue>> load() throws IOException, ConfigurationException {
        @SuppressWarnings("unchecked")
        Class<K> keyClass = (Class<K>) this.keyClass;
        
        return storage.load(context, keyClass, null);
    }

    public static CommandLine parseCommand(String[] args) {
        Options options = new Options();
        
        Option brokers = new Option("c", "conf", true, "path to configuration file");
        brokers.setRequired(true);
        options.addOption(brokers);
        
        options.addOption(new Option("d", "define-metric", true, "filter by defined metric id"));
        options.addOption(new Option("m", "monitor", true, "filter by monitor id"));
        options.addOption(new Option("n", "notificator", true, "filter by notificator id"));
        
        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);
            
            return cmd;
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("kafka-statuses-manager", options);

            System.exit(1);
            return null;
        }
    }

    protected void config(Properties properties, CommandLine cmd) throws ConfigurationException  {
        storage = ComponentManager.build(Type.STATUS_STORAGE, properties.getSubset(StatusesStorage.STATUS_STORAGE_PARAM));
        
        if(cmd.hasOption("define-metric"))
            keyClass = DefinedMetricStatuskey.class;
        else if(cmd.hasOption("monitor"))
            keyClass = MonitorStatusKey.class;
        else if(cmd.hasOption("notificator"))
            keyClass = NotificatorStatusKey.class;
        else
            keyClass = null;
    }

}
