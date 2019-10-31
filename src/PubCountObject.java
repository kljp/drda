import com.EPartition.EPartitionMessageSchema.msgEPartition;

public class PubCountObject {

    private msgEPartition subscription;
    private int pubCount;
    private double timestamp;

    public PubCountObject(msgEPartition subscription){

        this.subscription = subscription;
        this.pubCount = 0;
        this.timestamp = 0.0;
    }

    public msgEPartition getSubscription(){

        return this.subscription;
    }

    public void setPubCount(int pubCount){

        this.pubCount = pubCount;
    }

    public int getPubCount(){

        return this.pubCount;
    }

    public void setTimestamp(int timestamp){

        this.timestamp = timestamp;
    }

    public double getTimestamp(){

        return this.timestamp;
    }
}
