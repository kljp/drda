import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class LoadBalancerMasterNotfThread extends Thread {

    private ArrayList<Integer> wakeThread;
    private HashMap<String, LoadStatusObject> lsoMap;
    private double repDeg;

    public LoadBalancerMasterNotfThread(ArrayList<Integer> wakeThread, HashMap<String, LoadStatusObject> lsoMap, double repDeg) {

        this.wakeThread = wakeThread;
        this.lsoMap = lsoMap;
        this.repDeg = repDeg;
    }

    @Override
    public void run() {

        double before = System.currentTimeMillis();
        double after;
        double elapsed;

        while (true) {

            after = System.currentTimeMillis();
            elapsed = (after - before) / 1000.0;

            if (elapsed > GlobalState.PeriodOfSync) {

                wakeWorkThreads();
                waitWorkThreads();

                // calculate replication degree
                repDeg = 3;

                wakeWorkThreads();
                waitWorkThreads();

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
}
