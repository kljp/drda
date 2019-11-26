public class GlobalState {

    public static final int NumberOfDimensions = 8;
    public static String[] attributes = new String[]{"attr_0", "attr_1", "attr_2", "attr_3", "attr_4", "attr_5", "attr_6", "attr_7"}; // This is the order of the attributes.
    public static String[] attributes2 = new String[]{"attr_0", "attr_1", "attr_2", "attr_3", "attr_4", "attr_5", "attr_6", "attr_7"}; // This is the order of the attributes for test.
    public static final double[] minimumBounds = new double[]{0, 0, 0, 0, 0, 0, 0, 0};
    public static final double[] maximumBounds = new double[]{180, 180, 180, 180, 180, 180, 180, 180};
    public static int UnderThresholdOfRange = 0;
    public static int OverThresholdOfRange = 180;
    public static int NumberOfSegmentsPerDimension = 2; // segment degree
    public static final String[] segmentIdentifier = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"}; // Currently, we support totally 15 identifiers except zero.
    public static int NumberOfDimensionGroups = 1;
    public static int NumberOfDimensionsPerGroup = NumberOfDimensions / NumberOfDimensionGroups;
    public static final boolean[] skewedSubscriptionDist = new boolean[]{true, true, true, true, false, false, false, false};
    public static int NumberOfSubscriptions = 10000; // for pre-defined workload
    public static int NumberOfPublications = 10000; // for pre-defined workload
    public static final String subscriptionListDir = "./src/messages/subscriptions/subscription ";
    public static final String publicationListDir = "./src/messages/publications/publication ";
    public static int NumberOfBrokers = 8;
    public static int NumberOfChordNode = 32;
    public static String IPS_IP = "210.107.197.172";
    public static int IPS_LB_PORT = 5007;
    public static int IPS_BROKER_PORT = 5008;
    public static int IPS_CLIENT_PORT = 5009;
    public static int SUB_COUNT = 800;
    public static int PUB_COUNT = 10000;
    public static double PeriodOfSync = 7.0;
    public static double REP_DEG_INIT = 3.0;
    public static int MAX_NUM_BROKER = 1024;
    public static int PERIOD_SYNC_START = 5;
    public static int PERIOD_SYNC_END = 15;
    public static String EXP_MODE = "ON"; // "ON" or "OFF"
    public static String SKEWED_SUBSCRIPTION_MODE = "OFF"; // "ON" or "OFF"
    public static String SKEWED_PUBLICATION_MODE = "OFF"; // "ON" or "OFF"
    public static String LOAD_OPTION = "ALL"; // "SUB": only consider the number of subscriptions, "AC": only consider the number of access counts, "ALL": consider both the number of subscriptions and the number of access counts.
    public static String DRDA_MODE = "ON"; // "ON", "SEMI" or "OFF". SEMI: replication degree is always 3.
    public static String UNSUB_MODE = "OFF"; // "ON" or "OFF"
}