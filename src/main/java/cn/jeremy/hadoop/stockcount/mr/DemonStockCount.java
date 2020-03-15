package cn.jeremy.hadoop.stockcount.mr;

import cn.jeremy.hadoop.stockcount.mr.bean.DemonStock;
import cn.jeremy.hadoop.stockcount.mr.bean.JobStartFiled;
import cn.jeremy.hadoop.stockcount.mr.bean.RawStock;
import cn.jeremy.hadoop.stockcount.mr.mapper.RawStockMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 统计最后一个交易日股票涨幅大于3.33%，而至少前3个交易日股票涨跌幅小于最后一个交易日的1/3，这样的股票
 *
 * @author fengjiangtao
 * @date 2020/3/12 21:19
 */
public class DemonStockCount extends BaseStockCount
{

    @Override
    public String outPathTag()
    {
        return "demon";
    }

    static class DemonStockReducer extends Reducer<Text, RawStock, Text, DemonStock>
    {
        @Override
        protected void reduce(Text key, Iterable<RawStock> values, Context context)
            throws IOException, InterruptedException
        {
            Iterator<RawStock> iterator = values.iterator();
            List<RawStock> list = new ArrayList<>();
            while (iterator.hasNext())
            {
                RawStock next = iterator.next();
                try
                {
                    list.add(next.clone());
                }
                catch (CloneNotSupportedException e)
                {
                    //
                }
            }
            Collections.sort(list);
            RawStock lastRawStock = list.get(0);

            String lastNum = lastRawStock.getNum();
            String lastName = lastRawStock.getName();
            int lastChg = lastRawStock.getChg();
            int lastJe = lastRawStock.getJe();
            int count = 0;
            int chg = 0;
            int je = 0;
            if (lastChg > 333 && lastJe > 0)
            {
                for (int i = 1; i < list.size(); i++)
                {
                    RawStock next = list.get(i);
                    if (Math.abs(next.getChg()) < lastChg / 3)
                    {
                        count++;
                        chg += next.getChg();
                        je += next.getJe();
                    }
                    else
                    {
                        break;
                    }
                }
            }

            if (count >= 4 && lastJe > je && je > 0)
            {
                DemonStock demonStock = new DemonStock(lastNum,lastName,count,chg,je,lastChg,lastJe);
                context.write(new Text(demonStock.getNum()), demonStock);
            }
        }
    }

    /**
     * 需要传入3个参数
     * 1 原始数据位置
     * 2 输出文件位置
     * 3 从哪个时间往后统计 yyyy-MM-dd
     *
     * @param args
     * @author fengjiangtao
     */
    public static void main(String[] args)
        throws IOException, ClassNotFoundException, InterruptedException
    {
        String inPath = args[0];
        String outPath = args[1];
        String date = args[2];
        BaseStockCount stockCount = new DemonStockCount();
        stockCount.setCountDate(date);

        JobStartFiled jobStartFiled =
            new JobStartFiled(DemonStockCount.class,
                RawStockMapper.class,
                DemonStockReducer.class,
                Text.class,
                RawStock.class,
                Text.class,
                DemonStock.class,
                1,
                inPath,
                outPath);

        stockCount.start(jobStartFiled);
    }

}
