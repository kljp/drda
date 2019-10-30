import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class LoadBalancerMasterNotfThread extends Thread {

    private ArrayList<Integer> wakeThread;

    public LoadBalancerMasterNotfThread(ArrayList<Integer> wakeThread) {

        this.wakeThread = wakeThread;
    }

    @Override
    public void run() {

        double before = System.currentTimeMillis();
        double after;
        double elapsed;
        int countExit;

        while (true) {

            after = System.currentTimeMillis();
            elapsed = (after - before) / 1000.0;

            if (elapsed > 30) {

                synchronized (wakeThread) {
                    for (int i = 0; i < wakeThread.size(); i++)
                        wakeThread.set(i, 1);
                }

                while(true){ // wait for completion of collecting processes until every value within wakeThread is set to 0

                    countExit = 0;

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

                before = System.currentTimeMillis();
            }
        }
    }
}
