import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.util.ArrayList;
import java.util.HashMap;

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

    public msgEPartition[] preventDuplicates(msgEPartition[] ms, HashMap<Integer, String> IPMap){ // prevent duplicates of a subscription from being transferred to same broker.

        ArrayList<String> checkDuplicates = new ArrayList<String>();
        String tempStr;
        msgEPartition[] messages;
        ArrayList<msgEPartition> tempMessages = new ArrayList<msgEPartition>();

        for (int i = 0; i < ms.length; i++) {

            tempStr = IPMap.get(MurmurHash.hash32(ms[i].getSubspaceForward()) % IPMap.size());
            if(checkDuplicates.contains(tempStr))
                continue;
            else
                checkDuplicates.add(tempStr);

            tempMessages.add(ms[i]);
        }

        messages = new msgEPartition[tempMessages.size()];

        for (int i = 0; i < messages.length; i++)
            messages[i] = tempMessages.get(i);

        return messages;
    }
}
