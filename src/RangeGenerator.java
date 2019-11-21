public class RangeGenerator {

    public static final double[] minbounds = new double[GlobalState.NumberOfDimensions];
    public static final double[] maxbounds = new double[GlobalState.NumberOfDimensions];
    boolean[] skewedSubscription = new boolean[GlobalState.NumberOfDimensions];

    public RangeGenerator() {

        for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

            minbounds[i] = GlobalState.minimumBounds[i];
            maxbounds[i] = GlobalState.maximumBounds[i];
        }

        for (int i = 0; i < GlobalState.NumberOfDimensions; i++)
            skewedSubscription[i] = GlobalState.skewedSubscriptionDist[i]; // currently not used
    }

    public AttributeRanges randomRangeGenerator() {

        double[] lowerbounds = new double[GlobalState.NumberOfDimensions];
        double[] upperbounds = new double[GlobalState.NumberOfDimensions];

        double probability;
        double randomLowerBound;
        double randomUpperBound;
        double avgRands;

        for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

            while (true) {

                randomLowerBound = Math.random();
                randomUpperBound = Math.random();

                lowerbounds[i] = randomLowerBound * (maxbounds[i] - minbounds[i]) + minbounds[i];
                upperbounds[i] = randomUpperBound * (maxbounds[i] - minbounds[i]) + minbounds[i];

                // For skewed subscription dataset
                if(GlobalState.SKEWED_SUBSCRIPTION_MODE.equals("ON")){
                    if(i != 0){
                        if(Math.abs(lowerbounds[i] - lowerbounds[i - 1])  > 30 || Math.abs(upperbounds[i] - upperbounds[i - 1]) > 30)
                            continue;
                    }
                }

                if (lowerbounds[i] <= upperbounds[i]
                        && (upperbounds[i] - lowerbounds[i]) > GlobalState.UnderThresholdOfRange
                        && (upperbounds[i] - lowerbounds[i]) < GlobalState.OverThresholdOfRange) { // (GlobalState.maximumBounds[i] - GlobalState.minimumBounds[i]) / GlobalState.NumberOfSegmentsPerDimension
                    lowerbounds[i] = Math.round(lowerbounds[i] * 100) / 100.0;
                    upperbounds[i] = Math.round(upperbounds[i] * 100) / 100.0;
                    break;
                }
            }
        }

        return new AttributeRanges(lowerbounds, upperbounds);
    }

    public double[] randomValueGenerator() {

        double[] singlePoints = new double[GlobalState.NumberOfDimensions];

        for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

            while(true){

                singlePoints[i] = Math.random() * (maxbounds[i] - minbounds[i]) + minbounds[i];

                // For skewed publication dataset
                if(GlobalState.SKEWED_PUBLICATION_MODE.equals("ON")){
                    if(i != 0){
                        if(Math.abs(singlePoints[i] - singlePoints[i - 1])  > 30)
                            continue;
                    }
                }

                singlePoints[i] = Math.round(singlePoints[i] * 100) / 100.0;
                break;
            }
        }

        return singlePoints;
    }
}