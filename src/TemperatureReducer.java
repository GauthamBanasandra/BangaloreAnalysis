import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by gautham on 9/9/16.
 */
class TemperatureReducer extends Reducer<Text, CustomWritable, Text, DoubleWritable>
{
    @Override
    protected void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException
    {
        double maxValue = -10000;
        String maxSno=null;
        for (CustomWritable value : values)
            if (value.getTemperature().get() > maxValue)
            {
                maxSno=value.getSerialNumber().toString();
                maxValue = value.getTemperature().get();

                // Debug.
                System.out.println("value = " + value.getSerialNumber().toString() + "\t" + value.getTemperature().get());
            }

        context.write(new Text(maxSno), new DoubleWritable(maxValue));
    }
}
