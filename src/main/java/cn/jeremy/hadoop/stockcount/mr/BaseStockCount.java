package cn.jeremy.hadoop.stockcount.mr;

import cn.jeremy.hadoop.stockcount.mr.bean.JobStartFiled;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public abstract class BaseStockCount
{
    private Map<String, String> selfConf = new HashMap<>();

    private String countDate;

    public abstract String outPathTag();

    public void setSelfConf(String key, String value)
    {
        selfConf.put(key, value);
    }

    public void setCountDate(String value)
    {
        selfConf.put("count.date", value);
        this.countDate = value;
    }

    public void start(JobStartFiled jobStartFiled)
        throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        //设置自定义参数
        if (selfConf.size() > 0)
        {
            for (Entry<String, String> entry : selfConf.entrySet())
            {
                conf.set(entry.getKey(), entry.getValue());
            }
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(jobStartFiled.getJarCls());
        job.setMapperClass(jobStartFiled.getMapperCls());
        job.setReducerClass(jobStartFiled.getReducerCls());
        job.setMapOutputKeyClass(jobStartFiled.getMapperOutKeyCls());
        job.setMapOutputValueClass(jobStartFiled.getMapperOutValueCls());
        job.setOutputKeyClass(jobStartFiled.getOutPutKeyCls());
        job.setOutputValueClass(jobStartFiled.getOutPutValueCls());
        job.setNumReduceTasks(jobStartFiled.getReduceTasksNum());
        FileInputFormat.setInputPaths(job, new Path(jobStartFiled.getInputPath()));
        String outPathStr = jobStartFiled.getOutPath();
        outPathStr = countDate == null ? outPathStr : outPathStr.concat(countDate).concat("/");
        outPathStr = outPathTag() == null ? outPathStr : outPathStr.concat(outPathTag());
        Path outPath = new Path(outPathStr);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath))
        {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
