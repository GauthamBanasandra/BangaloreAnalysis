import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by gautham on 9/9/16.
 */
class TemperatureMapper extends Mapper<LongWritable, Text, Text, CustomWritable>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] row=value.toString().split(",");
        double temperature=Double.parseDouble(row[2].equals("Temperature")?"-1":row[2]);
        String serialNumber=row[0];

        CustomWritable writable=new CustomWritable();
        writable.setSerialNumber(new Text(serialNumber));
        writable.setTemperature(new DoubleWritable(temperature));

        // Debug.
        System.out.println("writable = " + writable);

        context.write(new Text("key"), writable);
    }
}
