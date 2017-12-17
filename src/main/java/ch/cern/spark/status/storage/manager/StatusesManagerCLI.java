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
import ch.cern.spark.json.JSONParser;
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
    
    private String defined_metric_id;
    private String monitor_id;
    private String notificator_id;
    private boolean printJSON;
    
    public StatusesManagerCLI() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("KafkaStatusesManagement");
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.driver.host", "localhost");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        
        context = new JavaSparkContext(sparkConf);
    }
    
    public static void main(String[] args) throws ConfigurationException, IOException {
        CommandLine cmd = parseCommand(args);
        if(cmd == null)
            return;
        
        Properties properties = Properties.fromFile(cmd.getOptionValue(""));
        
        StatusesManagerCLI manager = new StatusesManagerCLI();
        manager.config(properties, cmd);
        
        JavaRDD<Tuple2<StatusKey, StatusValue>> statuses = manager.load();
        
        manager.print(statuses);
    }

    private void print(JavaRDD<Tuple2<StatusKey, StatusValue>> statuses) {
        JavaRDD<String> toPrint = null;
        
        if(printJSON)
            toPrint = statuses.map(status -> JSONParser.parse(status).toString());
        else
            toPrint = statuses.map(status -> status.toString());
        
        toPrint.foreach(System.out::println);
    }

    public<K extends StatusKey> JavaRDD<Tuple2<K, StatusValue>> load() throws IOException, ConfigurationException {
        @SuppressWarnings("unchecked")
        Class<K> keyClass = (Class<K>) this.keyClass;
        
        JavaRDD<Tuple2<K, StatusValue>> allStatuses = storage.load(context, keyClass, null);
        
        return allStatuses
                    .filter(new DefinedMetricFilter<K>(defined_metric_id))
                    .filter(new MonitorFilter<K>(monitor_id))
                    .filter(new NotificatorFilter<K>(notificator_id));
    }

    public static CommandLine parseCommand(String[] args) {
        Options options = new Options();
        
        Option brokers = new Option("c", "conf", true, "path to configuration file");
        brokers.setRequired(true);
        options.addOption(brokers);
        
        options.addOption(new Option("d", "definedMetrics", false, "filter by defined metrics"));
        options.addOption(new Option("m", "monitors", false, "filter by monitors"));
        options.addOption(new Option("n", "notificators", false, "filter by notificators"));
        
        options.addOption(new Option("dID", "definedMetric", true, "filter by defined metric id"));
        options.addOption(new Option("mID", "monitor", true, "filter by monitor id"));
        options.addOption(new Option("nID", "notificator", true, "filter by notificator id"));
        
        options.addOption(new Option("json", "printJSON", false, "print as JSON"));
        
        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);
            
            return cmd;
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("kafka-statuses-manager", options);

            return null;
        }
    }

    protected void config(Properties properties, CommandLine cmd) throws ConfigurationException  {
        storage = ComponentManager.build(Type.STATUS_STORAGE, properties.getSubset(StatusesStorage.STATUS_STORAGE_PARAM));
        
        if(cmd.hasOption("definedMetrics"))
            keyClass = DefinedMetricStatuskey.class;
        else if(cmd.hasOption("monitors"))
            keyClass = MonitorStatusKey.class;
        else if(cmd.hasOption("notificators"))
            keyClass = NotificatorStatusKey.class;
        else
            keyClass = null;
        
        if(cmd.hasOption("definedMetric"))
            defined_metric_id = cmd.getOptionValue("definedMetric");
        if(cmd.hasOption("monitor"))
            monitor_id = cmd.getOptionValue("monitor");
        if(cmd.hasOption("notificator"))
            notificator_id = cmd.getOptionValue("notificator");
        
        printJSON = cmd.hasOption("printJSON");
    }
    
    public void close(){
        if(context != null)
            context.stop();
        context = null;
    }
    
}
