package cn.jeremy.hadoop.stockcount.mr;

import cn.jeremy.hadoop.stockcount.mr.bean.JobStartFiled;
import cn.jeremy.hadoop.stockcount.mr.bean.RawStock;
import cn.jeremy.hadoop.stockcount.mr.bean.StockFund;
import cn.jeremy.hadoop.stockcount.mr.bean.StockFundCount;
import cn.jeremy.hadoop.stockcount.mr.mapper.RawStockMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 统计股票多日资金情况
 *
 * @author fengjiangtao
 * @date 2020/3/13 22:21
 */
public class ContinuousStockFundCount extends BaseStockCount
{

    @Override
    public String outPathTag()
    {
        return "fund";
    }

    static class ContinuousStockFundReducer extends Reducer<Text, RawStock, Text, StockFund>
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
            List<StockFundCount> stockFundCounts = new ArrayList<>();
            int count = 0;
            int je = 0;
            for (RawStock rawStock : list)
            {
                count++;
                je += rawStock.getJe();
                if (count == 1 || count == 3 || count == 5 || count == 10 || count == 20 || count == 30)
                {
                    stockFundCounts.add(new StockFundCount(count, je));
                }
            }

            if (count != 1 && count != 3 && count != 5 && count != 10 && count != 20 && count != 30)
            {
                stockFundCounts.add(new StockFundCount(count, je));
            }
            StockFundCount[] array = new StockFundCount[stockFundCounts.size()];
            stockFundCounts.toArray(array);
            StockFund stockFund = new StockFund(list.get(0).getName());
            stockFund.getArrayWritable().set(array);
            context.write(new Text(list.get(0).getNum()), stockFund);
        }
    }

    /**
     * 需要传入32个参数
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
        BaseStockCount stockCount = new ContinuousStockFundCount();
        stockCount.setCountDate(date);

        JobStartFiled jobStartFiled =
            new JobStartFiled(ContinuousStockFundCount.class,
                RawStockMapper.class,
                ContinuousStockFundReducer.class,
                Text.class,
                RawStock.class,
                Text.class,
                StockFund.class,
                1,
                inPath,
                outPath);

        stockCount.start(jobStartFiled);
    }

}
