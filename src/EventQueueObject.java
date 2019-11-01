import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.util.Queue;

public class EventQueueObject {

    private int seqThread;
    private Queue<msgEPartition> eventQueue;

    public EventQueueObject(int seqThread, Queue<msgEPartition> eventQueue){

        this.seqThread = seqThread;
        this.eventQueue = eventQueue;
    }

    public int getSeqThread(){

        return this.seqThread;
    }

    public Queue<msgEPartition> getEventQueue(){

        return this.eventQueue;
    }
}
