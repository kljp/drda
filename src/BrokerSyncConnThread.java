import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class BrokerSyncConnThread extends  Thread{

    private ArrayList<PubCountObject> subscriptions;
    private int BROKER_PORT;

    public BrokerSyncConnThread(ArrayList<PubCountObject> subscriptions, int BROKER_PORT){

        this.subscriptions = subscriptions;
        this.BROKER_PORT = BROKER_PORT;
    }

    @Override
    public void run(){

        try {
            ServerSocket serverSocket = new ServerSocket(BROKER_PORT);

            while (true) {
                Socket socket = serverSocket.accept();
                new BrokerSyncProcThread(subscriptions, socket).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
