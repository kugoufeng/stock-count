package cn.jeremy.hadoop.stockcount.mr.mapper;

import cn.jeremy.hadoop.stockcount.mr.bean.RawStock;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RawStockMapper extends Mapper<LongWritable, Text, Text, RawStock>
{

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] split = line.split("\t");
        Date stockDate = timeStr2Date(split[11]);
        Configuration conf = context.getConfiguration();
        String countDate = conf.get("count.date");
        if (countDate != null)
        {
            Date date = timeStr2Date(countDate);
            if (date.compareTo(stockDate) >= 0)
            {
                RawStock rawStock = getRawStock(split);
                context.write(new Text(rawStock.getNum()), rawStock);
            }
        }else {
            RawStock rawStock = getRawStock(split);
            context.write(new Text(rawStock.getNum()), rawStock);
        }
    }

    private RawStock getRawStock(String[] split)
    {
        RawStock rawStock = new RawStock();
        rawStock.setName(split[0]);
        rawStock.setNum(split[1]);
        rawStock.setOpenPrice(Integer.valueOf(split[2]));
        rawStock.setTopPrice(Integer.valueOf(split[3]));
        rawStock.setLowPrice(Integer.valueOf(split[4]));
        rawStock.setYestClosePrice(Integer.valueOf(split[5]));
        rawStock.setClosePrice(Integer.valueOf(split[6]));
        rawStock.setChg(Integer.valueOf(split[7]));
        rawStock.setZlc(Integer.valueOf(split[8]));
        rawStock.setZlr(Integer.valueOf(split[9]));
        rawStock.setJe(Integer.valueOf(split[10]));
        rawStock.setToday(timeStr2Date(split[11]));
        return rawStock;
    }

    public static Date timeStr2Date(String time)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", new Locale("EN"));
        if (null == time)
        {
            throw new IllegalArgumentException("time parameter can not be null");
        }
        try
        {
            return sdf.parse(time);
        }
        catch (ParseException e)
        {
            throw new IllegalArgumentException("using [yyyy-MM-dd] parse [" + time + "] failed");
        }
    }

}
