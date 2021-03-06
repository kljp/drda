import java.io.Serializable;

public class LoadStatusObject implements Serializable {

    private String BROKER_IP;
    private int numSubscriptions;
    private int accessCount;

    public LoadStatusObject(){

    }

    public void setBROKER_IP(String BROKER_IP){

        this.BROKER_IP = BROKER_IP;
    }

    public String getBROKER_IP(){

        return this.BROKER_IP;
    }

    public void setNumSubscriptions(int numSubscriptions){

        this.numSubscriptions = numSubscriptions;
    }

    public int getNumSubscriptions(){

        return this.numSubscriptions;
    }

    public void setAccessCount(int accessCount){

        this.accessCount = accessCount;
    }

    public int getAccessCount(){

        return this.accessCount;
    }
}