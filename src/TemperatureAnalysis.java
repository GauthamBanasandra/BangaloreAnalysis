import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by gautham on 9/9/16.
 */
public class TemperatureAnalysis
{
    public static void main(String[] args)
    {
        if (args.length != 2)
        {
            System.err.println("Usage: TemperatureAnalysis <input path> <output path>");
            System.exit(-1);
        }

        try
        {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Bangalore temperature analysis");
            job.setJarByClass(TemperatureAnalysis.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(TemperatureMapper.class);
            job.setReducerClass(TemperatureReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e)
        {
            e.printStackTrace();
        }
    }
}
