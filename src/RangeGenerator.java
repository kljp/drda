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
            skewedSubscription[i] = GlobalState.skewedSubscriptionDist[i];
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
//                if(i != 0 && i != 4){
//
//                    if(Math.abs(lowerbounds[i] - lowerbounds[i - 1])  > 30 || Math.abs(upperbounds[i] - upperbounds[i - 1]) > 30)
//                        continue;
//                }

                if (lowerbounds[i] <= upperbounds[i] && (upperbounds[i] - lowerbounds[i]) < 20) { // (GlobalState.maximumBounds[i] - GlobalState.minimumBounds[i]) / GlobalState.NumberOfSegmentsPerDimension
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

            singlePoints[i] = Math.random() * (maxbounds[i] - minbounds[i]) + minbounds[i];
            singlePoints[i] = Math.round(singlePoints[i] * 100) / 100.0;
        }

        return singlePoints;
    }
}