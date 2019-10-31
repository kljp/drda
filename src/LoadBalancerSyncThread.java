import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class LoadBalancerSyncThread extends Thread{

    private String BROKER_IP;
    private int BROKER_PORT;

    public LoadBalancerSyncThread(String BROKER_IP, int BROKER_PORT){

        this.BROKER_IP = BROKER_IP;
        this.BROKER_PORT = BROKER_PORT;
    }

    @Override
    public void run(){

        Socket socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(BROKER_IP, BROKER_PORT));
            // poll
            // When initiated, send request and receive data
            // save it to shared data structure or any object
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
