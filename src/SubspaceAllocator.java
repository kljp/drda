import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.util.ArrayList;
import java.util.Arrays;

public class SubspaceAllocator {

    double[] sizeOfSegment = new double[GlobalState.NumberOfDimensions];
    String[] segID = new String[GlobalState.segmentIdentifier.length];

    public SubspaceAllocator() {

        for (int i = 0; i < GlobalState.NumberOfDimensions; i++)
            sizeOfSegment[i] = (GlobalState.maximumBounds[i] - GlobalState.minimumBounds[i]) / GlobalState.NumberOfSegmentsPerDimension;

        for (int i = 0; i < GlobalState.segmentIdentifier.length; i++)
            segID[i] = GlobalState.segmentIdentifier[i];
    }

    public msgEPartition allocateSubspace(msgEPartition m) {

        String msgType = m.getMsgType();
        int[] low = new int[GlobalState.NumberOfDimensionsPerGroup];
        int[] high = new int[GlobalState.NumberOfDimensionsPerGroup];
        int[] singlePoint = new int[GlobalState.NumberOfDimensionsPerGroup];
        String[] subspace;
        String[] tempSubspace;
        String subspacePub;
        String tempSubspacePub;
        ArrayList<String[]> subspaces = new ArrayList<>();
        ArrayList<String> subspacesPub = new ArrayList<>();
        int temp;

        if (msgType.equals("Subscription")) {

            for (int i = 0; i < GlobalState.NumberOfDimensionGroups; i++) {

                temp = 1;

                for (int j = 0; j < GlobalState.NumberOfDimensionsPerGroup; j++) {

                    low[j] = (int) ((m.getSub().getLowerBound(j + i * GlobalState.NumberOfDimensionsPerGroup)) / sizeOfSegment[j + i * GlobalState.NumberOfDimensionsPerGroup] + 1);
                    high[j] = (int) ((m.getSub().getUpperBound(j + i * GlobalState.NumberOfDimensionsPerGroup)) / sizeOfSegment[j + i * GlobalState.NumberOfDimensionsPerGroup] + 1);
                }

                for (int j = 0; j < GlobalState.NumberOfDimensionsPerGroup; j++)
                    temp *= (high[j] - low[j] + 1);

                subspace = new String[temp];

                for (int j = 0; j < subspace.length; j++)
                    subspace[j] = "";

                for (int j = 0; j < GlobalState.NumberOfDimensionsPerGroup; j++)
                    subspace = appendDimension(subspace, temp, low[j], high[j]);

                tempSubspace = new String[temp];

                for (int j = 0; j < tempSubspace.length; j++)
                    tempSubspace[j] = "";

                for (int j = 0; j < tempSubspace.length; j++) {
                    for (int k = 0; k < GlobalState.NumberOfDimensions; k++) {

                        if (k / GlobalState.NumberOfDimensionsPerGroup == i)
                            tempSubspace[j] += subspace[j].charAt(k - (k / GlobalState.NumberOfDimensionsPerGroup) * GlobalState.NumberOfDimensionsPerGroup);
                        else
                            tempSubspace[j] += "0";
                    }
                }

                subspaces.add(tempSubspace);
            }

            msgEPartition.Builder message = msgEPartition.newBuilder();
            message.mergeFrom(m);

            for (String[] strs : subspaces) {
                for (String str : strs)
                    message.addSubspace(str);
            }

            return message.build();

        } else if (msgType.equals("Publication")) {

            for (int i = 0; i < GlobalState.NumberOfDimensionGroups; i++) {

                subspacePub = "";

                for (int j = 0; j < GlobalState.NumberOfDimensionsPerGroup; j++)
                    subspacePub += segID[(int) ((m.getPub().getSinglePoint(j + i * GlobalState.NumberOfDimensionsPerGroup)) / sizeOfSegment[j + i * GlobalState.NumberOfDimensionsPerGroup] + 1)];

                tempSubspacePub = "";

                for (int j = 0; j < GlobalState.NumberOfDimensions; j++) {

                    if (j / GlobalState.NumberOfDimensionsPerGroup == i)
                        tempSubspacePub += subspacePub.charAt(j - (j / GlobalState.NumberOfDimensionsPerGroup) * GlobalState.NumberOfDimensionsPerGroup);
                    else
                        tempSubspacePub += "0";
                }

                subspacesPub.add(tempSubspacePub);
            }

            msgEPartition.Builder message = msgEPartition.newBuilder();
            message.mergeFrom(m);

            for (String str : subspacesPub)
                message.addSubspace(str);

            return message.build();

        } else {

            for (int i = 0; i < GlobalState.NumberOfDimensionGroups; i++) {

                temp = 1;

                for (int j = 0; j < GlobalState.NumberOfDimensionsPerGroup; j++) {

                    low[j] = (int) ((m.getUnsub().getLowerBound(j + i * GlobalState.NumberOfDimensionsPerGroup)) / sizeOfSegment[j + i * GlobalState.NumberOfDimensionsPerGroup] + 1);
                    high[j] = (int) ((m.getUnsub().getUpperBound(j + i * GlobalState.NumberOfDimensionsPerGroup)) / sizeOfSegment[j + i * GlobalState.NumberOfDimensionsPerGroup] + 1);
                }

                for (int j = 0; j < GlobalState.NumberOfDimensionsPerGroup; j++)
                    temp *= (high[j] - low[j] + 1);

                subspace = new String[temp];

                for (int j = 0; j < subspace.length; j++)
                    subspace[j] = "";

                for (int j = 0; j < GlobalState.NumberOfDimensionsPerGroup; j++)
                    subspace = appendDimension(subspace, temp, low[j], high[j]);

                tempSubspace = new String[temp];

                for (int j = 0; j < tempSubspace.length; j++)
                    tempSubspace[j] = "";

                for (int j = 0; j < tempSubspace.length; j++) {
                    for (int k = 0; k < GlobalState.NumberOfDimensions; k++) {

                        if (k / GlobalState.NumberOfDimensionsPerGroup == i)
                            tempSubspace[j] += subspace[j].charAt(k - (k / GlobalState.NumberOfDimensionsPerGroup) * GlobalState.NumberOfDimensionsPerGroup);
                        else
                            tempSubspace[j] += "0";
                    }
                }

                subspaces.add(tempSubspace);
            }

            msgEPartition.Builder message = msgEPartition.newBuilder();
            message.mergeFrom(m);

            for (String[] strs : subspaces) {
                for (String str : strs)
                    message.addSubspace(str);
            }

            return message.build();
        }
    }

    public String[] appendDimension(String[] subspace, int temp, int low, int high) {

        Arrays.sort(subspace);

        int count = 0;

        for (int i = 0; i < (temp / (high - low + 1)); i++) {
            for (int j = low; j <= high; j++) {
                subspace[(high - low + 1) * i + count] += segID[j];
                count++;
            }
            count = 0;
        }

        return subspace;
    }
}