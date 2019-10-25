import com.EPartition.EPartitionMessageSchema.msgEPartition.Unsubscription;

public class UnsubscriptionWrapper {

    double[] lowerbounds = new double[GlobalState.NumberOfDimensions];
    double[] upperbounds = new double[GlobalState.NumberOfDimensions];

    public UnsubscriptionWrapper(AttributeRanges attributeRanges){

        for(int i = 0; i < GlobalState.NumberOfDimensions; i++){

            this.lowerbounds[i] = attributeRanges.lowerbounds[i];
            this.upperbounds[i] = attributeRanges.upperbounds[i];
        }
    }
    public Unsubscription buildUnsubscription(){

        Unsubscription.Builder unsubscription = Unsubscription.newBuilder();

        for(int i = 0; i < GlobalState.NumberOfDimensions; i++){

            unsubscription.addLowerBound(lowerbounds[i]);
            unsubscription.addUpperBound(upperbounds[i]);
        }

        return unsubscription.build();
    }
}