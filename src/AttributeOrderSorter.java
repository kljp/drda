import com.EPartition.EPartitionMessageSchema.msgEPartition;
import com.EPartition.EPartitionMessageSchema.msgEPartition.Publication;
import com.EPartition.EPartitionMessageSchema.msgEPartition.Subscription;
import com.EPartition.EPartitionMessageSchema.msgEPartition.Unsubscription;

public class AttributeOrderSorter {

    AttributeOrder attributeOrder;

    public AttributeOrderSorter(AttributeOrder attributeOrder) {

        this.attributeOrder = attributeOrder;
    }

    public msgEPartition sortAttributeOrder(msgEPartition m) {

        int[] order;
        String[] attributes;
        double[] lowerBounds;
        double[] upperBounds;
        double[] singlePoints;

        int tempOrder;
        double tempLowerBound;
        double tempUpperBound;
        double tempSinglePoints;

        String msgType = m.getMsgType();

        if (msgType.equals("Subscription")) {

            order = new int[GlobalState.NumberOfDimensions];
            attributes = new String[GlobalState.NumberOfDimensions];
            lowerBounds = new double[GlobalState.NumberOfDimensions];
            upperBounds = new double[GlobalState.NumberOfDimensions];

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                attributes[i] = m.getAttribute(i);
                lowerBounds[i] = m.getSub().getLowerBound(i);
                upperBounds[i] = m.getSub().getUpperBound(i);
            }

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                for (int j = 0; j < GlobalState.NumberOfDimensions; j++) {

                    if (attributeOrder.attributes[i].equals(attributes[j]))
                        order[j] = attributeOrder.order[i];
                }
            }

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                for (int j = 0; j < GlobalState.NumberOfDimensions - i - 1; j++) {

                    if (order[j] > order[j + 1]) {

                        tempOrder = order[j + 1];
                        order[j + 1] = order[j];
                        order[j] = tempOrder;

                        tempLowerBound = lowerBounds[j + 1];
                        lowerBounds[j + 1] = lowerBounds[j];
                        lowerBounds[j] = tempLowerBound;

                        tempUpperBound = upperBounds[j + 1];
                        upperBounds[j + 1] = upperBounds[j];
                        upperBounds[j] = tempUpperBound;
                    }
                }
            }

            msgEPartition.Builder message = msgEPartition.newBuilder();
            Subscription.Builder subscription = Subscription.newBuilder();
            message.mergeFrom(m);

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                message.setAttribute(i, GlobalState.attributes[i]);
                subscription.addLowerBound(lowerBounds[i]);
                subscription.addUpperBound(upperBounds[i]);
            }

            subscription.setId(m.getSub().getId());
            message.setSub(subscription.build());

            return message.build();

        } else if (msgType.equals("Publication")) {

            order = new int[GlobalState.NumberOfDimensions];
            attributes = new String[GlobalState.NumberOfDimensions];
            singlePoints = new double[GlobalState.NumberOfDimensions];

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                attributes[i] = m.getAttribute(i);
                singlePoints[i] = m.getPub().getSinglePoint(i);
            }

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                for (int j = 0; j < GlobalState.NumberOfDimensions; j++) {

                    if (attributeOrder.attributes[i].equals(attributes[j]))
                        order[j] = attributeOrder.order[i];
                }
            }

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                for (int j = 0; j < GlobalState.NumberOfDimensions - i - 1; j++) {

                    if (order[j] > order[j + 1]) {

                        tempOrder = order[j + 1];
                        order[j + 1] = order[j];
                        order[j] = tempOrder;

                        tempSinglePoints = singlePoints[j + 1];
                        singlePoints[j + 1] = singlePoints[j];
                        singlePoints[j] = tempSinglePoints;
                    }
                }
            }

            msgEPartition.Builder message = msgEPartition.newBuilder();
            Publication.Builder publication = Publication.newBuilder();
            message.mergeFrom(m);

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                message.setAttribute(i, GlobalState.attributes[i]);
                publication.addSinglePoint(singlePoints[i]);
            }

            message.setPub(publication.build());

            return message.build();
        } else{

            order = new int[GlobalState.NumberOfDimensions];
            attributes = new String[GlobalState.NumberOfDimensions];
            lowerBounds = new double[GlobalState.NumberOfDimensions];
            upperBounds = new double[GlobalState.NumberOfDimensions];

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                attributes[i] = m.getAttribute(i);
                lowerBounds[i] = m.getUnsub().getLowerBound(i);
                upperBounds[i] = m.getUnsub().getUpperBound(i);
            }

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                for (int j = 0; j < GlobalState.NumberOfDimensions; j++) {

                    if (attributeOrder.attributes[i].equals(attributes[j]))
                        order[j] = attributeOrder.order[i];
                }
            }

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                for (int j = 0; j < GlobalState.NumberOfDimensions - i - 1; j++) {

                    if (order[j] > order[j + 1]) {

                        tempOrder = order[j + 1];
                        order[j + 1] = order[j];
                        order[j] = tempOrder;

                        tempLowerBound = lowerBounds[j + 1];
                        lowerBounds[j + 1] = lowerBounds[j];
                        lowerBounds[j] = tempLowerBound;

                        tempUpperBound = upperBounds[j + 1];
                        upperBounds[j + 1] = upperBounds[j];
                        upperBounds[j] = tempUpperBound;
                    }
                }
            }

            msgEPartition.Builder message = msgEPartition.newBuilder();
            Unsubscription.Builder unsubscription = Unsubscription.newBuilder();
            message.mergeFrom(m);

            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                message.setAttribute(i, GlobalState.attributes[i]);
                unsubscription.addLowerBound(lowerBounds[i]);
                unsubscription.addUpperBound(upperBounds[i]);
            }

            message.setUnsub(unsubscription.build());

            return message.build();

        }
    }
}