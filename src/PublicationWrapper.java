import com.EPartition.EPartitionMessageSchema.msgEPartition.Publication;

public class PublicationWrapper {

    double[] singlePoints = new double[GlobalState.NumberOfDimensions];

    public PublicationWrapper(double[] singlePoints){

        for(int i = 0; i < GlobalState.NumberOfDimensions; i++)
            this.singlePoints[i] = singlePoints[i];
    }

    public Publication buildPublication(){

        Publication.Builder publication = Publication.newBuilder();

        for(int i = 0; i < GlobalState.NumberOfDimensions; i++)
            publication.addSinglePoint(singlePoints[i]);

        return publication.build();
    }
}