import com.EPartition.EPartitionMessageSchema.msgEPartition;
import com.EPartition.EPartitionMessageSchema.msgEPartition.Publication;
import com.EPartition.EPartitionMessageSchema.msgEPartition.Subscription;
import com.EPartition.EPartitionMessageSchema.msgEPartition.Unsubscription;

public class MessageWrapper {

    String msgType;
    AttributeRanges attributeRanges;
    double[] singlePoints;

    public MessageWrapper(String msgType, AttributeRanges attributeRanges) {

        this.msgType = msgType;
        this.attributeRanges = attributeRanges;
    }

    public MessageWrapper(String msgType, double[] singlePoints) {

        this.msgType = msgType;
        this.singlePoints = singlePoints;
    }

    public msgEPartition buildMsgEPartition() {

        msgEPartition.Builder message = msgEPartition.newBuilder();
        message.setIPAddress("MyIP");
        message.setPayload("MyPayload");

        for (int i = 0; i < GlobalState.NumberOfDimensions; i++)
            message.addAttribute(GlobalState.attributes2[i]);

        if (msgType.equals("Subscription")) {

            message.setMsgType(msgType);
            SubscriptionWrapper subscriptionWrapper = new SubscriptionWrapper(attributeRanges);
            Subscription subscription = subscriptionWrapper.buildSubscription();
            message.setSub(subscription);
        } else if (msgType.equals("Publication")) {

            message.setMsgType(msgType);
            PublicationWrapper publicationWrapper = new PublicationWrapper(singlePoints);
            Publication publication = publicationWrapper.buildPublication();
            message.setPub(publication);
        } else if (msgType.equals("Unsubscription")) {

            message.setMsgType(msgType);
            UnsubscriptionWrapper unsubscriptionWrapper = new UnsubscriptionWrapper(attributeRanges);
            Unsubscription unsubscription = unsubscriptionWrapper.buildUnsubscription();
            message.setUnsub(unsubscription);
        }

        return message.build();
    }
}