import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by gautham on 9/9/16.
 */
public class TemperatureMapper extends Mapper<Text, Text, Text, DoubleWritable>
{
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] row=value.toString().split(",");
        double temperature=Double.parseDouble(row[2]);
        String serialNumber=row[0];
        context.write(new Text(serialNumber), new DoubleWritable(temperature));
    }
}
