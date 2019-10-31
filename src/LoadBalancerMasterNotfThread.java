import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class LoadBalancerMasterNotfThread extends Thread {

    private ArrayList<Integer> wakeThread;
    private ArrayList<ArrayList<String>> BrokerList;
    private HashMap<Integer, String> IPMap;

    public LoadBalancerMasterNotfThread(ArrayList<Integer> wakeThread, ArrayList<ArrayList<String>> BrokerList, HashMap<Integer, String> IPMap) {

        this.wakeThread = wakeThread;
        this.BrokerList = BrokerList;
        this.IPMap = IPMap;
    }

    @Override
    public void run() {

        double before = System.currentTimeMillis();
        double after;
        double elapsed;
        int countExit;
        int checkFirst = 0;

        while (true) {

            after = System.currentTimeMillis();
            elapsed = (after - before) / 1000.0;

            if (elapsed > GlobalState.PeriodOfSync) {

                if(checkFirst == 0){

                    BrokerList = new ArrayList<ArrayList<String>>();

                    for (int i = 0; i < wakeThread.size(); i++)
                        BrokerList.add(new ArrayList<String>());

                    countExit = 0;

                    for (int i = 0; i < IPMap.size(); i++) {

                        if(countExit == 1)
                            break;

                        for (int j = 0; j < wakeThread.size(); j++) {
                            if(j + i * wakeThread.size() < IPMap.size())
                                BrokerList.get(j).add(IPMap.get(j + i * wakeThread.size()));
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
