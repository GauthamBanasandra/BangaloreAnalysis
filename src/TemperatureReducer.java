import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by gautham on 9/9/16.
 */
class TemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
    {
        double max = Double.MIN_VALUE;

        for (DoubleWritable value : values)
            if (value.get() > max)
                max = value.get();

        context.write(key, new DoubleWritable(max));
    }
}
