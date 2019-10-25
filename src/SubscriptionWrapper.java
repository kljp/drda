import com.EPartition.EPartitionMessageSchema.msgEPartition.Subscription;

public class SubscriptionWrapper {

    double[] lowerbounds = new double[GlobalState.NumberOfDimensions];
    double[] upperbounds = new double[GlobalState.NumberOfDimensions];

    public SubscriptionWrapper(AttributeRanges attributeRanges){

        for(int i = 0; i < GlobalState.NumberOfDimensions; i++){

            this.lowerbounds[i] = attributeRanges.lowerbounds[i];
            this.upperbounds[i] = attributeRanges.upperbounds[i];
        }
    }
    public Subscription buildSubscription(){

        Subscription.Builder subscription = Subscription.newBuilder();

        for(int i = 0; i < GlobalState.NumberOfDimensions; i++){

            subscription.addLowerBound(lowerbounds[i]);
            subscription.addUpperBound(upperbounds[i]);
        }

        return subscription.build();
    }
}