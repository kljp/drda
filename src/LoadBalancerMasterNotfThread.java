import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class LoadBalancerMasterNotfThread extends Thread {

    private ArrayList<Integer> wakeThread;
    private ArrayList<ArrayList<String>> BrokerList;
    private HashMap<Integer, String> IPMap;
    private ReplicationDegree repDeg;
    private ArrayList<LoadStatusObject> lsos;
    private int BROKER_PORT;

    public LoadBalancerMasterNotfThread(ArrayList<Integer> wakeThread, ArrayList<ArrayList<String>> BrokerList, HashMap<Integer, String> IPMap, ReplicationDegree repDeg, ArrayList<LoadStatusObject> lsos, int BROKER_PORT) {

        this.wakeThread = wakeThread;
        this.BrokerList = BrokerList;
        this.IPMap = IPMap;
        this.repDeg = repDeg;
        this.lsos = lsos;
        this.BROKER_PORT = BROKER_PORT;
    }

    @Override
    public void run() {

        double before = System.currentTimeMillis();
        double after;
        double elapsed;
        int countExit;
        int checkFirst = 0;
        int tempSize; //the number of worker threads
        int tempNum; // the number of connected brokers
        int checkType = 0;

        while (true) {

            after = System.currentTimeMillis();
            elapsed = (after - before) / 1000.0;

            if (elapsed > GlobalState.PeriodOfSync) {

                if(checkFirst == 0){

                    synchronized (wakeThread){

                        if(wakeThread.size() == 0)
                            checkType = 0;
                        else
                            checkType = 1;
                    }
                }

                if(checkType == 0){ // The number of LB is 1.

                    if(checkFirst == 0){

                        synchronized (IPMap){

                            for (int i = 0; i < IPMap.size(); i++) {

                                synchronized (wakeThread) {
                                    wakeThread.add(0);
                                }

                                new LoadBalancerMasterSyncProcThread(IPMap.get(i), BROKER_PORT, wakeThread, i, lsos).start();
                            }
                        }

                        checkFirst = 1;
                    }

                    else{

                        wakeWorkThreads();
                        waitWorkThreads();

                        calculateReplicationDegree();
                    }
                }

                else {// The number of LB is more than 1.

                    if(checkFirst == 0){

                        BrokerList = new ArrayList<ArrayList<String>>();

                        synchronized (wakeThread){
                            tempSize = wakeThread.size();
                        }

                        synchronized (IPMap){
                            tempNum = IPMap.size();
                        }

                        for (int i = 0; i < tempSize; i++)
                            BrokerList.add(new ArrayList<String>());

                        countExit = 0;

                        for (int i = 0; i < tempNum; i++) {

                            if(countExit == 1)
                                break;

                            for (int j = 0; j < tempSize; j++) {
                                if(j + i * tempSize < tempNum){
                                    synchronized (IPMap){
                                        BrokerList.get(j).add(IPMap.get(j + i * tempSize));
                                    }
                                }
                                else{
                                    countExit = 1;
                                    break;
                                }
                            }
                        }

                        checkFirst = 1;
                    }

                    wakeWorkThreads();
                    waitWorkThreads();

                    calculateReplicationDegree();

                    wakeWorkThreads();
                    waitWorkThreads();
                }

                before = System.currentTimeMillis();
            }
        }
    }

    private void wakeWorkThreads(){

        synchronized (wakeThread) {
            for (int i = 0; i < wakeThread.size(); i++)
                wakeThread.set(i, 1);
        }
    }

    private void waitWorkThreads(){

        while(true){ // wait for completion of collecting processes until every value within wakeThread is set to 0

            int countExit = 0;

            synchronized (wakeThread){

                for (int i = 0; i < wakeThread.size(); i++) {

                    if(wakeThread.get(i) == 0)
                        countExit++;
                    else
                        break;
                }

                if(countExit == wakeThread.size())
                    break;
            }
        }
    }

    private void calculateReplicationDegree(){

        // calculate replication degree using lsos ArrayList

        synchronized (repDeg){ // should be replaced by actual calculated value
            repDeg.setRepDegDouble(GlobalState.REP_DEG_INIT);
            repDeg.setRepDegInt((int) GlobalState.REP_DEG_INIT);
        }
    }
}
