public class AttributeRanges {

    double[] lowerbounds = new double[GlobalState.NumberOfDimensions];
    double[] upperbounds = new double[GlobalState.NumberOfDimensions];
    double[] singlePoints = new double[GlobalState.NumberOfDimensions];

    public AttributeRanges(double[] lowerbounds, double[] upperbounds)
    {
        for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

            this.lowerbounds[i] = lowerbounds[i];
            this.upperbounds[i] = upperbounds[i];
        }
    }
}