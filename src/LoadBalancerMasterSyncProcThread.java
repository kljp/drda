import com.EPartition.GlobalSyncObject.SyncObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;

public class LoadBalancerMasterSyncProcThread extends Thread {

    private String BROKER_IP;
    private int BROKER_PORT;
    private ArrayList<Integer> wakeThread;
    private int threadId;
    private ArrayList<LoadStatusObject> tempLsos;

    public LoadBalancerMasterSyncProcThread(String BROKER_IP, int BROKER_PORT, ArrayList<Integer> wakeThread, int threadId, ArrayList<LoadStatusObject> tempLsos) {

        this.BROKER_IP = BROKER_IP;
        this.BROKER_PORT = BROKER_PORT;
        this.wakeThread = wakeThread;
        this.threadId = threadId;
        this.tempLsos = tempLsos;
    }

    @Override
    public void run() {

        Socket socket = new Socket();
        DataOutputStream dataOutputStream = null;
        DataInputStream dataInputStream = null;
        LoadStatusObject lso;

        try {
            socket.connect(new InetSocketAddress(BROKER_IP, BROKER_PORT));
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataInputStream = new DataInputStream(socket.getInputStream());

            while (true) {

                synchronized (wakeThread) {

                    if (wakeThread.get(threadId) == 1) {

                        dataOutputStream.writeUTF("sync");
                        dataOutputStream.flush();

                        applyLocalLso(dataInputStream);

                        wakeThread.set(threadId, 0);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void applyLocalLso(DataInputStream dataInputStream) {

        SyncObject syncObject;

        try {
            syncObject = SyncObject.parseDelimitedFrom(dataInputStream);

            synchronized (tempLsos) {

                LoadStatusObject lso = new LoadStatusObject();
                lso.setBROKER_IP(BROKER_IP);
                lso.setNumSubscriptions(syncObject.getLso(0).getNumSubscriptions());
                lso.setAccessCount(syncObject.getLso(0).getAccessCount());
                tempLsos.add(lso);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
