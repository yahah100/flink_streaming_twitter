package course.predictions;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.stream.DoubleStream;

/**
 * Simple Moving AVG
 */
public class AvgPrediction {

    private static int maxCount = 100;
    private int count;
    private double[] values;

    public AvgPrediction() {
        values = new double[maxCount];
        Arrays.fill(values, 0.D);
        count = 0;
    }


    public int predictAvgTweetLength(int length){

        double sum = DoubleStream.of(values).sum();
        double avg = sum / maxCount;
        if (Double.isNaN(avg)) {
            return -1;
        }
        else {
            return (int)avg;
        }
    }

    private void updateAVG(double length){
        values[count] = length;
        count ++;
        if (count == maxCount) count=0;
    }


    public void refineModel(int length) {
       updateAVG(length);
    }

}
