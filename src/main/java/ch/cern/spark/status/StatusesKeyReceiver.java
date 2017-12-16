package ch.cern.spark.status;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import ch.cern.spark.metrics.Driver;
import ch.cern.spark.status.storage.JSONStatusSerializer;

public class StatusesKeyReceiver extends Receiver<StatusKey> {

    private static final long serialVersionUID = -3082306224466741384L;
    
    private transient final static Logger LOG = Logger.getLogger(Driver.class.getName());

    private String host;
    private int port;

    private JSONStatusSerializer derializer;
    
    public StatusesKeyReceiver(String host, int port) {
        super(StorageLevel.MEMORY_ONLY());
        
        this.host = host;
        this.port = port;
        
        derializer = new JSONStatusSerializer();
    }

    @Override
    public void onStart() {
        LOG.info("Socket listening for removing statuses: " + host + ":" + port);
        
        new Thread() {
            @Override
            public void run() {
                listen();
            }
        }.start();;
    }
    
    private void listen() {
        while(!isStopped())
            try {
                tryReceive();

                Thread.sleep(5000);
            } catch (Throwable e) {}
    }  
    
    public void tryReceive() throws Throwable{
        Socket socket = new Socket(host, port);        
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        
        String line;
        while ((line = reader.readLine()) != null) {
            try {
                StatusKey key = derializer.toKey(line.getBytes());
                
                store(key);
            } catch(Exception e) {
                LOG.error("Statuses removal socket: " + e.getMessage(), e);
            }
        }
        
        reader.close();
        socket.close();
    }

    @Override
    public void onStop() {
    }

}
