import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class BrokerSyncProcThread extends Thread{

    private ArrayList<PubCountObject> subscriptions;
    private Socket socket;

    public BrokerSyncProcThread(ArrayList<PubCountObject> subscriptions, Socket socket){

        this.subscriptions = subscriptions;
        this.socket = socket;
    }

    @Override
    public void run(){

        int temp;
        LoadStatusObject lso;
        int accessCount;

        try {
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());

            while(true){

                temp = dataInputStream.readInt();

                if(temp == 1){

                    lso = new LoadStatusObject();
                    accessCount = 0;

                    synchronized (subscriptions){

                        lso.setNumSubscriptions(subscriptions.size());

                        for (int i = 0; i < subscriptions.size(); i++)
                            accessCount = accessCount + subscriptions.get(i).getPubCount();
                    }

                    lso.setAccessCount(accessCount);
                    objectOutputStream.writeObject(lso);
                    objectOutputStream.flush();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
