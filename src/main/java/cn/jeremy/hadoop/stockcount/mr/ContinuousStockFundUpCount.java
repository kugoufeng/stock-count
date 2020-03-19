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
 * 统计股票多日资金连续流入的股票
 *
 * @author fengjiangtao
 * @date 2020/3/13 22:21
 */
public class ContinuousStockFundUpCount extends BaseStockCount
{

    @Override
    public String outPathTag()
    {
        return "fund-up";
    }

    static class ContinuousStockFundUpReducer extends Reducer<Text, RawStock, Text, StockFund>
    {
        @Override
        protected void reduce(Text key, Iterable<RawStock> values, Context context)
            throws IOException, InterruptedException
        {
            List<RawStock> list = sortRawStockList(values, context.getConfiguration(), true);
            if (null == list) {
                return;
            }
            List<StockFundCount> stockFundCounts = new ArrayList<>();
            int count = 0;
            int je = 0;
            for (RawStock rawStock : list)
            {
                count++;
                je += rawStock.getJe();
                if (count == 1 || count == 3 || count == 5 || count == 10 || count == 20)
                {
                    stockFundCounts.add(new StockFundCount(count, je));
                }
            }

            if (count != 1 && count != 3 && count != 5 && count != 10 && count < 20)
            {
                stockFundCounts.add(new StockFundCount(count, je));
            }

            if (stockFundCounts.size() >= 5 && stockFundCounts.get(stockFundCounts.size() - 1).getCount() > 0)
            {
                boolean flag = true;
                for (int i = 0; i < stockFundCounts.size() - 1; i++)
                {
                    if (stockFundCounts.get(i).getCount() < stockFundCounts.get(i + 1).getCount())
                    {
                        flag = false;
                        break;
                    }
                }
                if (flag)
                {
                    StockFundCount[] array = new StockFundCount[stockFundCounts.size()];
                    stockFundCounts.toArray(array);
                    StockFund stockFund = new StockFund(list.get(0).getName());
                    stockFund.getArrayWritable().set(array);
                    context.write(new Text(list.get(0).getNum()), stockFund);
                }
            }
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
        BaseStockCount stockCount = new ContinuousStockFundUpCount();
        stockCount.setCountDate(date);

        JobStartFiled jobStartFiled =
            new JobStartFiled(ContinuousStockFundUpCount.class,
                RawStockMapper.class,
                ContinuousStockFundUpReducer.class,
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
