import com.EPartition.GlobalSyncObject.SyncObject;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class LoadBalancerMasterWorkThread extends Thread {

    private Socket socket;
    private ArrayList<Integer> wakeThread;
    private int threadId;
    private ArrayList<ArrayList<String>> BrokerList;
    private HashMap<Integer, String> IPMap;
    private ReplicationDegree repDeg;
    private ArrayList<LoadStatusObject> tempLsos;
    private ArrayList<LoadStatusObject> lsos;
    private int curSync;

    public LoadBalancerMasterWorkThread(Socket socket, ArrayList<Integer> wakeThread, int threadId, ArrayList<ArrayList<String>> BrokerList, HashMap<Integer, String> IPMap,
                                        ReplicationDegree repDeg, ArrayList<LoadStatusObject> tempLsos, ArrayList<LoadStatusObject> lsos, int curSync) {

        this.socket = socket;
        this.wakeThread = wakeThread;
        this.threadId = threadId;
        this.BrokerList = BrokerList;
        this.IPMap = IPMap;
        this.repDeg = repDeg;
        this.tempLsos = tempLsos;
        this.lsos = lsos;
        this.curSync = curSync;
    }

    @Override
    public void run() {

        DataOutputStream dataOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
        DataInputStream dataInputStream = null;
        int checkFirst = 0;
        int preventDeadlock = 0;
        int preventDeadlock2 = 0;
        SyncObject syncObject;
        SyncObject.Builder syncObjectBuilder;
        SyncObject.ReplicationDegree.Builder rdb;
        SyncObject.LoadStatusObject.Builder lsob;

        try {
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            dataInputStream = new DataInputStream(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {

            synchronized (wakeThread) {
                if (wakeThread.get(threadId) == 1)
                    preventDeadlock = 1;
            }

            if (preventDeadlock == 1) {

                // send request as string to the corresponding LB
                try {
                    if (checkFirst == 0) { // only come in when initiated

                        dataOutputStream.writeUTF("connect");
                        dataOutputStream.flush();
                        objectOutputStream.writeObject(BrokerList.get(threadId));
                        objectOutputStream.flush();

                        checkFirst = 1;
                    } else {

                        dataOutputStream.writeUTF("reduce");
                        dataOutputStream.flush();

                        fillTempLsos(dataInputStream);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

                synchronized (wakeThread) {
                    wakeThread.set(threadId, 0);
                }

                while (true) {

                    synchronized (wakeThread) {
                        if (wakeThread.get(threadId) == 1)
                            preventDeadlock2 = 1;
                    }
                    if (preventDeadlock2 == 1) {

                        try {

                            syncObjectBuilder = SyncObject.newBuilder();
                            rdb = SyncObject.ReplicationDegree.newBuilder();

                            syncObjectBuilder.setCurSync(curSync);

                            synchronized (repDeg) {
                                rdb.setRepDegDouble(repDeg.getRepDegDouble());
                                rdb.setRepDegInt(repDeg.getRepDegInt());
                                syncObjectBuilder.setRepDeg(rdb);
                            }

                            synchronized (lsos) {
                                for (int i = 0; i < lsos.size(); i++) {
                                    lsob = SyncObject.LoadStatusObject.newBuilder();
                                    lsob.setBROKERIP(lsos.get(i).getBROKER_IP());
                                    lsob.setNumSubscriptions(lsos.get(i).getNumSubscriptions());
                                    lsob.setAccessCount(lsos.get(i).getAccessCount());
                                    syncObjectBuilder.addLso(lsob);
                                }
                            }

                            syncObject = syncObjectBuilder.build();
                            syncObject.writeDelimitedTo(dataOutputStream);
                            dataOutputStream.flush();

                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        synchronized (wakeThread) {
                            wakeThread.set(threadId, 0);
                        }

                        preventDeadlock2 = 0;
                        break;
                    }
                }

                preventDeadlock = 0;
            }
        }
    }

    public void fillTempLsos(DataInputStream dataInputStream) {

        SyncObject syncObject;
        ArrayList<LoadStatusObject> tempLso = new ArrayList<LoadStatusObject>();

        try {
            syncObject = SyncObject.parseDelimitedFrom(dataInputStream);

            for (int i = 0; i < syncObject.getLsoList().size(); i++) {
                LoadStatusObject lso = new LoadStatusObject();
                lso.setBROKER_IP(syncObject.getLso(i).getBROKERIP());
                lso.setNumSubscriptions(syncObject.getLso(i).getNumSubscriptions());
                lso.setAccessCount(syncObject.getLso(i).getAccessCount());
                tempLso.add(lso);
            }

            synchronized (tempLsos){
                tempLsos.addAll(tempLso);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

