import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;

/**
 * Created by gautham on 9/9/16.
 */
public class TemperatureAnalysis
{
    private static final String BASE_URI = "hdfs://localhost:9000/";

    public static void main(String[] args)
    {
        if (args.length != 2)
        {
            System.err.println("Usage: TemperatureAnalysis <input path> <output path>");
            System.exit(-1);
        }

        try
        {
            BasicConfigurator.configure();
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Bangalore temperature analysis");
            job.setJarByClass(TemperatureAnalysis.class);

            URI inputUri = URI.create(BASE_URI + args[0]);
            URI outputUri = URI.create(BASE_URI + args[1]);

            Path outputPath = new Path(args[1]);
            FileSystem hadoopFs = FileSystem.get(outputUri, conf);
            if (hadoopFs.exists(outputPath))
                hadoopFs.delete(outputPath);

            FileInputFormat.addInputPath(job, new Path(inputUri));
            FileOutputFormat.setOutputPath(job, new Path(outputUri));

            job.setMapperClass(TemperatureMapper.class);
            job.setReducerClass(TemperatureReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(CustomWritable.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e)
        {
            e.printStackTrace();
        }
    }
}
