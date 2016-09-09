import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by gautham on 9/9/16.
 */
class CustomWritable implements WritableComparable<CustomWritable>
{
    private Text serialNumber;
    private DoubleWritable temperature;

    CustomWritable()
    {
        this.serialNumber = new Text();
        this.temperature = new DoubleWritable();
    }

    public CustomWritable(Text serialNumber, DoubleWritable temperature)
    {
        this.serialNumber = serialNumber;
        this.temperature = temperature;
    }

    Text getSerialNumber()
    {
        return serialNumber;
    }

    void setSerialNumber(Text serialNumber)
    {
        this.serialNumber = serialNumber;
    }

    DoubleWritable getTemperature()
    {
        return temperature;
    }

    void setTemperature(DoubleWritable temperature)
    {
        this.temperature = temperature;
    }

    @Override
    public int compareTo(CustomWritable o)
    {
        if (this.temperature.get() == o.temperature.get())
            return 0;
        else if (this.temperature.get() > o.temperature.get())
            return 1;
        else
            return -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        this.serialNumber.write(dataOutput);
        this.temperature.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        this.serialNumber.readFields(dataInput);
        this.temperature.readFields(dataInput);
    }

    @Override
    public int hashCode()
    {
        return this.serialNumber.hashCode();
    }

    @Override
    public String toString()
    {
        return this.serialNumber + "\t" + this.temperature;
    }
}
