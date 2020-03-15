package cn.jeremy.hadoop.stockcount.mr.bean;

import lombok.Data;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

@Data
public class JobStartFiled
{
    private Class<?> jarCls;
    private Class<? extends Mapper> mapperCls;
    private Class<? extends Reducer> reducerCls;
    private Class<?> mapperOutKeyCls;
    private Class<?> mapperOutValueCls;
    private Class<?> outPutKeyCls;
    private Class<?> outPutValueCls;
    private Integer reduceTasksNum;
    private String inputPath;
    private String outPath;

    public JobStartFiled(Class<?> jarCls, Class<? extends Mapper> mapperCls,
        Class<? extends Reducer> reducerCls, Class<?> mapperOutKeyCls, Class<?> mapperOutValueCls,
        Class<?> outPutKeyCls, Class<?> outPutValueCls, Integer reduceTasksNum, String inputPath, String outPath)
    {
        this.jarCls = jarCls;
        this.mapperCls = mapperCls;
        this.reducerCls = reducerCls;
        this.mapperOutKeyCls = mapperOutKeyCls;
        this.mapperOutValueCls = mapperOutValueCls;
        this.outPutKeyCls = outPutKeyCls;
        this.outPutValueCls = outPutValueCls;
        this.reduceTasksNum = reduceTasksNum;
        this.inputPath = inputPath;
        this.outPath = outPath;
    }
}
