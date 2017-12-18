package ch.cern.spark.status.storage.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.SparkConf;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import ch.cern.spark.status.storage.JavaStatusSerializer;
import ch.cern.spark.status.storage.StatusSerializer;
import ch.cern.spark.status.storage.StatusesStorage;
import scala.Tuple2;

public class StatusesManagerCLI {
    
    private StatusesStorage storage;
    private JavaSparkContext context;
        
    private String filter_by_id;
    private String filter_by_fqcn;
    
    private StatusSerializer serializer;
    
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
        
        Properties properties = Properties.fromFile(cmd.getOptionValue("conf"));
        
        StatusesManagerCLI manager = new StatusesManagerCLI();
        manager.config(properties, cmd);
        
        JavaPairRDD<StatusKey, StatusValue> filteredStatuses = manager.loadAndFilter();
        
        Map<Integer, StatusKey> indexedKeys = getInxedKeys(filteredStatuses);
        manager.printKeys(indexedKeys);
        
        int index = askForIndex();
        
        StatusKey key = indexedKeys.get(index);
        List<StatusValue> value = filteredStatuses.lookup(key);
        if(key == null || value.size() < 1) {
            System.out.println("There is no value for this key.");
            System.exit(1);
        }
        
        manager.printDetailedInfo(key, value.get(0));
    }

    private void printDetailedInfo(StatusKey key, StatusValue value) throws IOException {
        System.out.println();
        System.out.println("Detailed information:");
        System.out.println("Key: " + new String(serializer.fromKey(key)));
        System.out.println("Value: " + new String(serializer.fromValue(value)));
    }

    private static int askForIndex() {
        System.out.println();
        
        Scanner reader = new Scanner(System.in);
        
        System.out.println("Index number for detailed information (or exit): ");
        String indexString = reader.nextLine();
        if(indexString == null || indexString.equals("exit"))
            System.exit(0);
        
        int index = -1;
        try {
            index = Integer.parseInt(indexString);
        }catch(Exception e) {
            System.out.println("Wrong number: " + indexString);
            
            System.exit(1);
        }
        
        reader.close();
        
        return index;
    }

    private static Map<Integer, StatusKey> getInxedKeys(JavaPairRDD<StatusKey, StatusValue> filteredStatuses) {
        List<StatusKey> keys = filteredStatuses.map(tuple -> tuple._1).collect();
        
        Map<Integer, StatusKey> index = new HashMap<>();
        int i = 0;
        for (StatusKey statusKey : keys)
            index.put(i++, statusKey);
            
        return index;
    }

    private void printKeys(Map<Integer, StatusKey> indexedKeys) throws IOException {
        System.out.println("List of found keys:");
        
        for (Map.Entry<Integer, StatusKey> key : indexedKeys.entrySet())
            System.out.println(key.getKey() + ": " + new String(serializer.fromKey(key.getValue())));
    }

    public JavaPairRDD<StatusKey, StatusValue> loadAndFilter() throws IOException, ConfigurationException {
        JavaRDD<Tuple2<StatusKey, StatusValue>> statuses = storage.load(context);
        
        if(filter_by_id != null)
            statuses = statuses.filter(new IDStatusKeyFilter(filter_by_id));
        
        if(filter_by_fqcn != null)
            statuses = statuses.filter(new ClassNameStatusKeyFilter(filter_by_fqcn));
        
        return statuses.mapToPair(tuple -> tuple);
    }

    public static CommandLine parseCommand(String[] args) {
        Options options = new Options();
        
        Option brokers = new Option("c", "conf", true, "path to configuration file");
        brokers.setRequired(true);
        options.addOption(brokers);
        
        options.addOption(new Option("id", "id", true, "filter by status key id"));
        options.addOption(new Option("fqcn", "fqcn", true, "filter by FQCN or alias"));
        
        options.addOption(new Option("print", "print", false, "print as JSON"));
        
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
        
        filter_by_id = cmd.getOptionValue("id");
        filter_by_fqcn = cmd.getOptionValue("fqcn");
        
        if(!cmd.hasOption("print"))
            serializer = new JSONStatusSerializer();
        else if(cmd.getOptionValue("print").equals("java"))
            serializer = new JavaStatusSerializer();
        else if(cmd.getOptionValue("print").equals("json"))
            serializer = new JSONStatusSerializer();
        else
            throw new ConfigurationException("Print option " + cmd.getOptionValue("print") + " is not available");
    }
    
    public void close(){
        if(context != null)
            context.stop();
        context = null;
    }
    
}
