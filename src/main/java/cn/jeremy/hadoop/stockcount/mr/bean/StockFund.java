package cn.jeremy.hadoop.stockcount.mr.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Data;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

@Data
public class StockFund implements Writable
{

    private String name;

    private ArrayWritable arrayWritable = new ArrayWritable(StockFundCount.class);

    public StockFund()
    {
    }

    public StockFund(String name)
    {
        this.name = name;
    }


    @Override
    public void write(DataOutput dataOutput)
        throws IOException
    {
        dataOutput.writeUTF(name);
        arrayWritable.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput)
        throws IOException
    {
        this.name = dataInput.readUTF();
        arrayWritable.readFields(dataInput);
    }

    @Override
    public String toString()
    {
        final StringBuffer sb = new StringBuffer();
        sb.append(name).append('\t');
        for (Writable writable : arrayWritable.get())
        {
            sb.append(writable.toString()).append("\t");
        }
        String str = sb.substring(0, sb.length() - 1);
        return str.concat("\n");
    }
}
