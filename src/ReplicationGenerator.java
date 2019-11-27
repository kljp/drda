import com.EPartition.EPartitionMessageSchema.msgEPartition;
import com.EPartition.EPartitionMessageSchema.msgEPartition.Subscription;

import java.util.ArrayList;
import java.util.HashMap;

public class ReplicationGenerator {

    public ReplicationGenerator(){

    }

    public msgEPartition setIPAddress(msgEPartition m, String remoteHostName){

        msgEPartition.Builder messageBuilder = msgEPartition.newBuilder();
        messageBuilder.mergeFrom(m);
        messageBuilder.setIPAddress(remoteHostName);

        if(m.getMsgType().equals("Subscription")){

            Subscription.Builder subscriptionBuilder = Subscription.newBuilder();
            subscriptionBuilder.setId(remoteHostName + "_" + m.getSub().getId());
            subscriptionBuilder.addAllLowerBound(m.getSub().getLowerBoundList());
            subscriptionBuilder.addAllUpperBound(m.getSub().getUpperBoundList());
            Subscription subscription = subscriptionBuilder.build();
            messageBuilder.setSub(subscription);
        }

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

    public msgEPartition[] applyReplicationDegree(msgEPartition[] ms, HashMap<Integer, String> IPMap, ArrayList<LoadStatusObject> lsos, int repDeg, int checkFirst){

        msgEPartition[] messages;
        String tempStr;
        LoadStatusObject[] lsoArray;
        int[] loads;
        int temp;
        int count = 0;
        LoadStatusObject tempLso;

        messages = new msgEPartition[repDeg];

        if(checkFirst == 0){

            ArrayList<Integer> checkDuplicate = new ArrayList<Integer>();

            for (int i = 0; i < messages.length; i++) {
                while(true){
                    temp = ((int) (Math.random() * GlobalState.MAX_NUM_BROKER) % IPMap.size()) % ms.length;

                    if(!checkDuplicate.contains(temp)){
                        messages[i] = ms[temp];
                        checkDuplicate.add(temp);
                        break;
                    }
                }
            }

            return messages;
        }

        if(GlobalState.DIST_MODE.equals("LFSUB") || GlobalState.DIST_MODE.equals("LFAC") || GlobalState.DIST_MODE.equals("LFALL")){

            synchronized (lsos){
                lsoArray = lsos.toArray(new LoadStatusObject[lsos.size()]);
            }

            loads = new int[lsoArray.length];

            if(GlobalState.DIST_MODE.equals("LFSUB")){
                for (int i = 0; i < lsoArray.length; i++)
                    loads[i] = lsoArray[i].getNumSubscriptions();
            }
            else if(GlobalState.DIST_MODE.equals("LFAC")){
                for (int i = 0; i < lsoArray.length; i++)
                    loads[i] = lsoArray[i].getAccessCount();
            }
            else if(GlobalState.DIST_MODE.equals("LFALL")){
                for (int i = 0; i < lsoArray.length; i++)
                    loads[i] = lsoArray[i].getNumSubscriptions() * lsoArray[i].getAccessCount();
            }

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
        }

        else if(GlobalState.DIST_MODE.equals("RAND")){

            int[] indexes = new int[repDeg];

            for (int i = 0; i < repDeg; i++) {
                indexes[i] = (int) (Math.random() * ms.length);

                for (int j = 0; j < i; j++) {
                    if(indexes[i] == indexes[j]){
                        i--;
                        break;
                    }
                }
            }

            for (int i = 0; i < repDeg; i++)
                messages[i] = ms[indexes[i]];
        }

        else if(GlobalState.DIST_MODE.equals("PBSUB") || GlobalState.DIST_MODE.equals("PBAC") || GlobalState.DIST_MODE.equals("PBALL")){

            int loadsTotal = 0;
            int[] prob;

            synchronized (lsos){
                lsoArray = lsos.toArray(new LoadStatusObject[lsos.size()]);
            }

            loads = new int[lsoArray.length];
            prob = new int[loads.length];

            if(GlobalState.DIST_MODE.equals("PBSUB")){
                for (int i = 0; i < lsoArray.length; i++)
                    loads[i] = lsoArray[i].getNumSubscriptions();
            }
            else if(GlobalState.DIST_MODE.equals("PBAC")){
                for (int i = 0; i < lsoArray.length; i++)
                    loads[i] = lsoArray[i].getAccessCount();
            }
            else if(GlobalState.DIST_MODE.equals("PBALL")){
                for (int i = 0; i < lsoArray.length; i++)
                    loads[i] = lsoArray[i].getNumSubscriptions() * lsoArray[i].getAccessCount();
            }

            for (int i = 0; i < loads.length; i++)
                loadsTotal += loads[i];

            for (int i = 0; i < loads.length; i++)
                prob[i] = 100 * (int) ((1.0 - ((double) loads[i] / loadsTotal)) * (1.0 / (loads.length - 1)));

            ArrayList<Integer> probs = new ArrayList<Integer>();

            for (int i = 0; i < prob.length; i++) {
                for (int j = 0; j < prob[i]; j++) {
                    probs.add(i);
                }
            }

            int[] indexes = new int[repDeg];

            try{
                for (int i = 0; i < repDeg; i++) {
                    indexes[i] = probs.get((int) (Math.random() % probs.size()));

                    for (int j = 0; j < i; j++) {
                        if(indexes[i] == indexes[j]){
                            i--;
                            break;
                        }
                    }
                }
            } catch(IndexOutOfBoundsException e){
                for (int i = 0; i < prob.length; i++) {
                    System.out.println(i + " " + prob[i]);
                }
            }

            for (int i = 0; i < indexes.length; i++) {

                if(count == repDeg)
                    break;

                for (int j = 0; j < ms.length; j++) {

                    tempStr = IPMap.get(Math.abs(MurmurHash.hash32(ms[j].getSubspaceForward())) % IPMap.size());

                    if(tempStr.equals(lsoArray[indexes[i]].getBROKER_IP())){

                        messages[count] = ms[j];
                        count++;

                        break;
                    }
                }
            }
        }

        return messages;
    }

    public msgEPartition setGlobalSub(msgEPartition[] ms, HashMap<Integer, String> IPMap){

        String tempStr;
        msgEPartition msgGlobal;
        msgEPartition.Builder msgGlobalBuilder = msgEPartition.newBuilder();

        msgGlobalBuilder.mergeFrom(ms[0]);

        for (int i = 0; i < ms.length; i++) {

            synchronized (IPMap){
                tempStr = IPMap.get(Math.abs(MurmurHash.hash32(ms[i].getSubspaceForward())) % IPMap.size());
            }

            msgGlobalBuilder.addBrokers(tempStr);
        }

        msgGlobal = msgGlobalBuilder.build();

        return  msgGlobal;
    }
}
