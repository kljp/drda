import com.EPartition.GlobalSyncObject.SyncObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;

public class LoadBalancerSyncThread extends Thread{

    private String BROKER_IP;
    private int threadId;
    private int BROKER_PORT;
    private ArrayList<InitiatePollObject> checkPoll;
    private ArrayList<LoadStatusObject> sharedLsos;

    public LoadBalancerSyncThread(String BROKER_IP, int threadId, int BROKER_PORT, ArrayList<InitiatePollObject> checkPoll, ArrayList<LoadStatusObject> sharedLsos){

        this.BROKER_IP = BROKER_IP;
        this.threadId = threadId;
        this.BROKER_PORT = BROKER_PORT;
        this.checkPoll = checkPoll;
        this.sharedLsos = sharedLsos;
    }

    @Override
    public void run(){

        Socket socket = new Socket();
        DataOutputStream dataOutputStream = null;
        DataInputStream dataInputStream = null;
        LoadStatusObject lso;

        try {
            socket.connect(new InetSocketAddress(BROKER_IP, BROKER_PORT));
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataInputStream = new DataInputStream(socket.getInputStream());
            // poll
            while(true){
                synchronized (checkPoll.get(threadId)){

                    if(checkPoll.get(threadId).getCheck() == 1){

                        dataOutputStream.writeUTF("sync");
                        dataOutputStream.flush();

                        applyLocalLso(dataInputStream);

                        checkPoll.get(threadId).setCheck(0);
                    }
                }
            }
            // When initiated, send request and receive data
            // save it to shared data structure or any object
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void applyLocalLso(DataInputStream dataInputStream) {

        SyncObject syncObject;

        try {
            syncObject = SyncObject.parseDelimitedFrom(dataInputStream);
            System.out.println(syncObject);

            LoadStatusObject lso = new LoadStatusObject();
            lso.setBROKER_IP(BROKER_IP);
            lso.setNumSubscriptions(syncObject.getLso(0).getNumSubscriptions());
            lso.setAccessCount(syncObject.getLso(0).getAccessCount());

            synchronized (sharedLsos) {
                sharedLsos.add(lso);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
