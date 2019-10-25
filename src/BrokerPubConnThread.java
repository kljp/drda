import com.EPartition.EPartitionMessageSchema.msgEPartition;
import java.util.Queue;

public class BrokerPubConnThread extends Thread{

    private String LB_IP;
    private int LB_PORT;
    private Queue<msgEPartition> queue;
    private String serverType;

    public BrokerPubConnThread(String LB_IP, int LB_PORT, Queue<msgEPartition> queue, String serverType){

        this.LB_IP = LB_IP;
        this.LB_PORT = LB_PORT;
        this.queue = queue;
        this.serverType = serverType;
    }

    @Override
    public void run() {

        if(serverType.equals("loadbalancer"))
            new BrokerPubRecvThread(LB_IP, LB_PORT, queue).start();
    }
}
