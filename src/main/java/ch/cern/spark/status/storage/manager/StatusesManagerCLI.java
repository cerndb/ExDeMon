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

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.SparkConf;
import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.metrics.monitors.MonitorStatusKey;
import ch.cern.spark.metrics.notificator.NotificatorStatusKey;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.types.KafkaStatusesStorage;
import scala.Tuple2;

public class StatusesManagerCLI {
    
    private KafkaStatusesStorage storage;
    private JavaSparkContext context;
    private Class<? extends StatusKey> keyClass;
    
    public StatusesManagerCLI() {
        storage = new KafkaStatusesStorage();
        
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("KafkaStatusesManagement");
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.driver.host", "localhost");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        
        context = new JavaSparkContext(sparkConf);
    }
    
    public static void main(String[] args) throws ConfigurationException, IOException {
        StatusesManagerCLI manager = new StatusesManagerCLI();
        manager.config(args);
        
        JavaRDD<Tuple2<StatusKey, StatusValue>> statuses = manager.load();
        
        statuses.foreach(status -> System.out.println(status));
    }

    public<K extends StatusKey> JavaRDD<Tuple2<K, StatusValue>> load() throws IOException, ConfigurationException {
        @SuppressWarnings("unchecked")
        Class<K> keyClass = (Class<K>) this.keyClass;
        
        return storage.load(context, keyClass, null);
    }

    private CommandLine parseCommand(String[] args) {
        Options options = new Options();
        
        Option brokers = new Option("b", "brokers", true, "list of brokers host:port,host:port");
        brokers.setRequired(true);
        options.addOption(brokers);
        
        Option topic = new Option("t", "topic", true, "name of status topic");
        topic.setRequired(true);
        options.addOption(topic);
        
        options.addOption(new Option("d", "define-metrics", false, "manage defined metrics"));
        options.addOption(new Option("m", "monitors", false, "manage monitors"));
        options.addOption(new Option("n", "notificators", false, "manage notificators"));
        
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

    protected void config(String[] args) throws ConfigurationException  {
        CommandLine cmd = parseCommand(args);
        
        Properties properties = new Properties();
        properties.setProperty("topic", cmd.getOptionValue("topic"));
        properties.setProperty("producer.bootstrap.servers", cmd.getOptionValue("brokers"));
        properties.setProperty("consumer.bootstrap.servers", cmd.getOptionValue("brokers"));
        storage.config(properties);
        
        if(cmd.hasOption("define-metrics"))
            keyClass = DefinedMetricStatuskey.class;
        else if(cmd.hasOption("monitors"))
            keyClass = MonitorStatusKey.class;
        else if(cmd.hasOption("notificators"))
            keyClass = NotificatorStatusKey.class;
        else
            keyClass = null;
    }

}
