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

            tempStr = IPMap.get(Math.abs(MurmurHash.hash32(ms[i].getSubspaceForward())) % IPMap.size());
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

    public msgEPartition[] applyReplicationDegree(msgEPartition[] ms, HashMap<Integer, String> IPMap, ArrayList<LoadStatusObject> lsos, int repDeg){

        msgEPartition[] messages;
        String tempStr;
        LoadStatusObject[] lsoArray;
        int[] loads;
        int temp;
        int count = 0;
        LoadStatusObject tempLso;

        messages = new msgEPartition[repDeg];

        synchronized (lsos){
            lsoArray = lsos.toArray(new LoadStatusObject[lsos.size()]);
        }

        loads = new int[lsoArray.length];

        for (int i = 0; i < lsoArray.length; i++)
            loads[i] = lsoArray[i].getNumSubscriptions() * lsoArray[i].getAccessCount();

        // currently, it is implemented by the least-loaded broker selection with considering both the number of subscription and the access count.
        // In the future, more options should be additionally implemented: 1. random, 2. probabilistic, 3. only considering the number of subscription + every strategy.
        for (int i = 0; i < loads.length; i++) {

            for (int j = 0; j < loads.length - i - 1; j++) {

                if(loads[j] > loads[j + 1]){

                    temp = loads[j + 1];
                    loads[j + 1] = loads[j];
                    loads[j] = temp;

                    tempLso = lsoArray[j + 1];
                    lsoArray[j + 1] = lsoArray[j];
                    lsoArray[j] = tempLso;
                }
            }
        }

        for (int i = 0; i < lsoArray.length; i++) {

            if(count == repDeg)
                break;

            for (int j = 0; j < ms.length; j++) {

                tempStr = IPMap.get(Math.abs(MurmurHash.hash32(ms[j].getSubspaceForward())) % IPMap.size());

                if(tempStr.equals(lsoArray[i].getBROKER_IP())){

                    messages[count] = ms[j];
                    count++;

                    break;
                }
            }
        }

        return messages;
    }
}
