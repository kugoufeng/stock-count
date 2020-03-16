package cn.jeremy.hadoop.stockcount.mr.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Data;
import org.apache.hadoop.io.Writable;

@Data
public class StockFundCount implements Writable
{
    private int day;

    private int count;

    public StockFundCount()
    {
    }

    public StockFundCount(int day, int count)
    {
        this.day = day;
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput)
        throws IOException
    {
        dataOutput.writeInt(day);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput)
        throws IOException
    {
        this.day = dataInput.readInt();
        this.count = dataInput.readInt();
    }

    @Override
    public String toString()
    {
        final StringBuffer sb = new StringBuffer();
        sb.append(day).append("|");
        sb.append(count);
        return sb.toString();
    }
}
