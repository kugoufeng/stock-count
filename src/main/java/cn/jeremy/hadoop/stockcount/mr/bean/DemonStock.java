package cn.jeremy.hadoop.stockcount.mr.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Data;
import org.apache.hadoop.io.Writable;

@Data
public class DemonStock implements Writable
{
    /** 股票编号*/
    private String num;

    /** 股票名称*/
    private String name;

    /** 股票的潜伏天数*/
    private int day;

    /** 潜伏期股票的波动程度*/
    private int chg;

    /** 潜伏期间资金净流入*/
    private int je;

    /** 最后一个交易日，股票涨幅*/
    private int lastChg;

    /** 最后一个交易日，资金净流入*/
    private int lastJe;

    public DemonStock()
    {
    }

    public DemonStock(String num, String name, int day, int chg, int je, int lastChg, int lastJe)
    {
        this.num = num;
        this.name = name;
        this.day = day;
        this.chg = chg;
        this.je = je;
        this.lastChg = lastChg;
        this.lastJe = lastJe;
    }

    @Override
    public void write(DataOutput dataOutput)
        throws IOException
    {
        dataOutput.writeUTF(num);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(day);
        dataOutput.writeInt(je);
        dataOutput.writeInt(lastChg);
        dataOutput.writeInt(lastJe);
    }

    @Override
    public void readFields(DataInput dataInput)
        throws IOException
    {
        this.num = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.day = dataInput.readInt();
        this.je = dataInput.readInt();
        this.lastChg = dataInput.readInt();
        this.lastJe = dataInput.readInt();
    }

    @Override
    public String toString()
    {
        final StringBuffer sb = new StringBuffer();
        sb.append(name).append("\t");
        sb.append(day).append("\t");
        sb.append(chg).append("\t");
        sb.append(je).append("\t");
        sb.append(lastChg).append("\t");
        sb.append(lastJe).append("\n");
        return sb.toString();
    }
}
