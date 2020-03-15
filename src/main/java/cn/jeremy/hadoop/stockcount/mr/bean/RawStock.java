package cn.jeremy.hadoop.stockcount.mr.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import lombok.Data;
import org.apache.hadoop.io.Writable;

@Data
public class RawStock implements Writable, Comparable<RawStock>,Cloneable
{
    /**
     * 股票名称
     */
    private String name;

    /**
     * 股票代号
     */
    private String num;

    /**
     * 股票开盘价格
     */
    private int openPrice;

    /**
     * 股票最高价格
     */
    private int topPrice;

    /**
     * 股票最低价格
     */
    private int lowPrice;

    /**
     * 昨天收盘价格
     */
    private int yestClosePrice;

    /**
     * 股票收盘价格
     */
    private int closePrice;

    /**
     * 股票涨跌
     */
    private int chg;

    /**
     * 资金总流出（单位百元）
     */
    private int zlc;

    /**
     * 资金总流入（单位百元）
     */
    private int zlr;

    /**
     * 资金净额（单位百元）
     */
    private int je;

    /**
     * 当天日期
     */
    private Date today;

    @Override
    public void write(DataOutput dataOutput)
        throws IOException
    {
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(num);
        dataOutput.writeInt(openPrice);
        dataOutput.writeInt(topPrice);
        dataOutput.writeInt(lowPrice);
        dataOutput.writeInt(yestClosePrice);
        dataOutput.writeInt(closePrice);
        dataOutput.writeInt(chg);
        dataOutput.writeInt(zlc);
        dataOutput.writeInt(zlr);
        dataOutput.writeInt(je);
        dataOutput.writeLong(today.getTime());

    }

    @Override
    public void readFields(DataInput dataInput)
        throws IOException
    {
        this.name = dataInput.readUTF();
        this.num = dataInput.readUTF();
        this.openPrice = dataInput.readInt();
        this.topPrice = dataInput.readInt();
        this.lowPrice = dataInput.readInt();
        this.yestClosePrice = dataInput.readInt();
        this.closePrice = dataInput.readInt();
        this.chg = dataInput.readInt();
        this.zlc = dataInput.readInt();
        this.zlr = dataInput.readInt();
        this.je = dataInput.readInt();
        this.today = new Date(dataInput.readLong());
    }

    @Override
    public String toString()
    {
        final StringBuffer sb = new StringBuffer();
        sb.append(name).append("\t");
        sb.append(num).append("\t");
        sb.append(openPrice).append("\t");
        sb.append(topPrice).append("\t");
        sb.append(lowPrice).append("\t");
        sb.append(yestClosePrice).append("\t");
        sb.append(closePrice).append("\t");
        sb.append(chg).append("\t");
        sb.append(zlc).append("\t");
        sb.append(zlr).append("\t");
        sb.append(je).append("\t");
        sb.append(date2TimeStr(today, "yyyy-MM-dd")).append("\n");
        return sb.toString();
    }

    public  String date2TimeStr(Date time, String pattern)
    {
        if (null == pattern)
        {
            throw new IllegalArgumentException("pattern parameter can not be null");
        }
        if (null == time)
        {
            throw new IllegalArgumentException("time parameter can not be null");
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, new Locale("EN"));
        return sdf.format(time);
    }

    @Override
    public int compareTo(RawStock o)
    {
        return this.getToday().compareTo(o.getToday())*-1;
    }

    @Override
    public RawStock clone()
        throws CloneNotSupportedException
    {
        return (RawStock)super.clone();
    }
}
