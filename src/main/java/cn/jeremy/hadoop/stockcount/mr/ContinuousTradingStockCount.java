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
 * 统计连续两日及以上涨停的股票
 *
 * @author fengjiangtao
 * @date 2020/3/13 22:21
 */
public class ContinuousTradingStockCount extends BaseStockCount
{

    @Override
    public String outPathTag()
    {
        return "c-trading";
    }

    static class ContinuousTradingStockReducer extends Reducer<Text, RawStock, Text, DemonStock>
    {
        @Override
        protected void reduce(Text key, Iterable<RawStock> values, Context context)
            throws IOException, InterruptedException
        {
            List<RawStock> list = sortRawStockList(values, context.getConfiguration(), true);
            if (null == list) {
                return;
            }
            RawStock lastRawStock = list.get(0);

            String lastNum = lastRawStock.getNum();
            String lastName = lastRawStock.getName();
            int lastChg = lastRawStock.getChg();
            int lastJe = lastRawStock.getJe();
            int count = 0;
            int chg = 0;
            int je = 0;
            if (lastChg > 980)
            {
                for (int i = 0; i < list.size(); i++)
                {
                    RawStock next = list.get(i);
                    if (next.getChg() > 980)
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

            if (count >= 2)
            {
                DemonStock demonStock = new DemonStock(lastNum,lastName,count,chg,je,lastChg,lastJe);
                context.write(new Text(demonStock.getNum()), demonStock);
            }
        }
    }

    /**
     * 需要传入32个参数
     *  1 原始数据位置
     *  2 输出文件位置
     *  3 从哪个时间往后统计 yyyy-MM-dd
     *
     * @param args
     * @return
     * @throws
     * @author fengjiangtao
     */
    public static void main(String[] args)
        throws IOException, ClassNotFoundException, InterruptedException
    {
        String inPath = args[0];
        String outPath = args[1];
        String date = args[2];
        BaseStockCount stockCount = new ContinuousTradingStockCount();
        stockCount.setCountDate(date);

        JobStartFiled jobStartFiled =
            new JobStartFiled(ContinuousTradingStockCount.class,
                RawStockMapper.class,
                ContinuousTradingStockReducer.class,
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
