import com.EPartition.GlobalSyncObject.SyncObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

        String temp;
        LoadStatusObject lso;
        int accessCount;

        SyncObject syncObject;
        SyncObject.Builder syncObjectBuilder;
        SyncObject.LoadStatusObject.Builder lsob;

        try {
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

            while(true){

                temp = dataInputStream.readUTF();

                if(temp.equals("sync")){

                    lso = new LoadStatusObject();
                    accessCount = 0;

                    synchronized (subscriptions){

                        lso.setNumSubscriptions(subscriptions.size());

                        for (int i = 0; i < subscriptions.size(); i++)
                            accessCount = accessCount + subscriptions.get(i).getPubCount();
                    }

                    lso.setAccessCount(accessCount);

                    syncObjectBuilder = SyncObject.newBuilder();
                    lsob = SyncObject.LoadStatusObject.newBuilder();
                    lsob.setNumSubscriptions(lso.getNumSubscriptions());
                    lsob.setAccessCount(lso.getAccessCount());
                    syncObjectBuilder.addLso(lsob);
                    syncObject = syncObjectBuilder.build();
                    syncObject.writeDelimitedTo(dataOutputStream);
                    dataOutputStream.flush();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
