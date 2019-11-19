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
    private ArrayList<LoadStatusObject> tempLsos;
    private int BROKER_PORT;
    private static ArrayList<Double> repDegHistory = new ArrayList<Double>();
    private int curSync;

    public LoadBalancerMasterNotfThread(ArrayList<Integer> wakeThread, ArrayList<ArrayList<String>> BrokerList, HashMap<Integer, String> IPMap,
                                        ReplicationDegree repDeg, ArrayList<LoadStatusObject> lsos, ArrayList<LoadStatusObject> tempLsos, int BROKER_PORT, int curSync) {

        this.wakeThread = wakeThread;
        this.BrokerList = BrokerList;
        this.IPMap = IPMap;
        this.repDeg = repDeg;
        this.lsos = lsos;
        this.tempLsos = tempLsos;
        this.BROKER_PORT = BROKER_PORT;
        this.curSync = curSync;
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
        double beforeSync = 0.0;
        double afterSync;
        double elapsedSync;
        ArrayList<LoadStatusObject> lsoSyncStart = new ArrayList<LoadStatusObject>();

        while (true) {

            if(curSync == GlobalState.PERIOD_SYNC_START)
                beforeSync = System.currentTimeMillis();

            after = System.currentTimeMillis();
            elapsed = (after - before) / 1000.0;

            if (elapsed > GlobalState.PeriodOfSync) {

                curSync++;

                synchronized (tempLsos) {
                    tempLsos.clear();
                }

                if (checkFirst == 0) {

                    synchronized (wakeThread) {

                        if (wakeThread.size() == 0)
                            checkType = 0;
                        else
                            checkType = 1;
                    }
                }

                if (checkType == 0) { // The number of LB is 1.

                    if (checkFirst == 0) {

                        synchronized (IPMap) {

                            for (int i = 0; i < IPMap.size(); i++) {

                                synchronized (wakeThread) {
                                    wakeThread.add(0);
                                }

                                new LoadBalancerMasterSyncProcThread(IPMap.get(i), BROKER_PORT, wakeThread, i, tempLsos).start();
                            }
                        }

                        checkFirst = 1;
                    }

                    wakeWorkThreads();
                    waitWorkThreads();

                    calculateReplicationDegree();

                } else {// The number of LB is more than 1.

                    if (checkFirst == 0) {

//                        BrokerList = new ArrayList<ArrayList<String>>();

                        synchronized (wakeThread) {
                            tempSize = wakeThread.size();
                        }

                        synchronized (IPMap) {
                            tempNum = IPMap.size();
                        }

                        for (int i = 0; i < tempSize; i++)
                            BrokerList.add(new ArrayList<String>());

                        countExit = 0;

                        for (int i = 0; i < tempNum; i++) {

                            if (countExit == 1)
                                break;

                            for (int j = 0; j < tempSize; j++) {
                                if (j + i * tempSize < tempNum) {
                                    synchronized (IPMap) {
                                        BrokerList.get(j).add(IPMap.get(j + i * tempSize));
                                    }
                                } else {
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

                    synchronized (lsos) {
                        lsos.clear();
                        lsos.addAll(tempLsos);
                    }

                    wakeWorkThreads();
                    waitWorkThreads();
                }

                if(curSync == GlobalState.PERIOD_SYNC_START)
                    lsoSyncStart.addAll(tempLsos);

                System.out.println("curSync = " + curSync);

                synchronized (repDeg){
                    System.out.println(repDeg.getRepDegDouble() + " " + repDeg.getRepDegInt());
                }

                synchronized (lsos) {
                    if (!lsos.isEmpty()) {
                        for (int i = 0; i < lsos.size(); i++)
                            System.out.println(lsos.get(i).getBROKER_IP() + " " + lsos.get(i).getNumSubscriptions() + " " + lsos.get(i).getAccessCount());
                    }
                }

                System.out.println();

                before = System.currentTimeMillis();

                if(GlobalState.EXP_MODE.equals("ON")){

                    if(curSync == GlobalState.PERIOD_SYNC_END){

                        afterSync = System.currentTimeMillis();
                        elapsedSync = (afterSync - beforeSync) / 1000.0;
                        int numEvent = 0;
                        double matchingRate;

                        synchronized (lsos){
                            for (int i = 0; i < lsos.size(); i++)
                                numEvent = numEvent + (lsos.get(i).getAccessCount() - lsoSyncStart.get(i).getAccessCount());
                        }

                        matchingRate = (double) numEvent / elapsedSync;
                        System.out.println("Matching rate between period " + GlobalState.PERIOD_SYNC_START + " and " + GlobalState.PERIOD_SYNC_END + " is " + matchingRate + "(elapsed time = " + elapsedSync +")");

                        return;
                    }
                }
            }
        }
    }

    private void wakeWorkThreads() {

        synchronized (wakeThread) {
            for (int i = 0; i < wakeThread.size(); i++)
                wakeThread.set(i, 1);
        }
    }

    private void waitWorkThreads() {

        ArrayList<Integer> tempWakeThread;

        while (true) { // wait for completion of collecting processes until every value within wakeThread is set to 0

            int countExit = 0;

            synchronized (wakeThread){
                tempWakeThread = wakeThread;
            }

            for (int i = 0; i < tempWakeThread.size(); i++) {

                if (tempWakeThread.get(i) == 0)
                    countExit++;
                else
                    break;
            }

            if (countExit == tempWakeThread.size())
                break;

        }
    }

    private void calculateReplicationDegree() {

        int lsosSize;
        int[] nss; // An array of total subscription numbers of brokers
        int[] acs; // An array of the access counts of brokers
        double nssMean;
        double nssNormStdDev;
        double acsMean;
        double acsNormStdDev;
        double tempRepDeg;

        synchronized (tempLsos) {

            lsosSize = tempLsos.size();

            nss = new int[lsosSize];
            acs = new int[lsosSize];

            for (int i = 0; i < lsosSize; i++) {

                nss[i] = tempLsos.get(i).getNumSubscriptions();
                acs[i] = tempLsos.get(i).getAccessCount();
            }
        }

        nssMean = calculateMean(nss);
        nssNormStdDev = calculateStdDev(nss, nssMean) / nssMean;

        acsMean = calculateMean(acs);
        acsNormStdDev = calculateStdDev(acs, acsMean) / acsMean;

        tempRepDeg = 2.0 * ((double) IPMap.size()) * nssNormStdDev * acsNormStdDev;
        repDegHistory.add(tempRepDeg); // for experimental results

        if (tempRepDeg < 3.0 || Double.isNaN(tempRepDeg))
            tempRepDeg = 3.0;

        synchronized (repDeg) {

            repDeg.setRepDegDouble(tempRepDeg);
            repDeg.setRepDegInt((int) tempRepDeg);
        }
    }

    private double calculateMean(int[] array) {

        double sum = 0.0;

        for (int i = 0; i < array.length; i++)
            sum += (double) array[i];

        return sum / array.length;
    }

    private double calculateStdDev(int[] array, double mean) {

        double sum = 0.0;
        double stdDev;
        double diff;

        for (int i = 0; i < array.length; i++) {

            diff = (double) array[i] - mean;
            sum += diff * diff;
        }

        stdDev = Math.sqrt(sum / array.length);

        return stdDev;
    }
}
