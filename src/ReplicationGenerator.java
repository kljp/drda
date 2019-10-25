import com.EPartition.EPartitionMessageSchema.msgEPartition;

public class ReplicationGenerator {

    public ReplicationGenerator(){

    }

    public msgEPartition setIPAddress(msgEPartition m, String remoteHostName){

        msgEPartition.Builder messageBuilder = msgEPartition.newBuilder();
        messageBuilder.mergeFrom(m);
        messageBuilder.setIPAddress(remoteHostName);
        m = messageBuilder.build();

        return m;
    }

    public msgEPartition[] generateReplicates(msgEPartition m){

        int numSubspaces = m.getSubspaceList().size();
        msgEPartition.Builder[] messageBuilders = new msgEPartition.Builder[numSubspaces];
        msgEPartition[] messages = new msgEPartition[numSubspaces];

        for (int i = 0; i < m.getSubspaceList().size(); i++) {

            messageBuilders[i] = msgEPartition.newBuilder();
            messageBuilders[i].mergeFrom(m);
            messageBuilders[i].setSubspaceForward(m.getSubspace(i));
            messages[i] = messageBuilders[i].build();
        }

        return messages;
    }
}
