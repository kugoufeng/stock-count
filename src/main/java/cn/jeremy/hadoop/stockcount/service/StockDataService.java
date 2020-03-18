package cn.jeremy.hadoop.stockcount.service;

import cn.jeremy.common.utils.DateTools;
import cn.jeremy.common.utils.FileUtil;
import cn.jeremy.common.utils.HttpTools;
import cn.jeremy.common.utils.ZipUtil;
import cn.jeremy.common.utils.bean.HttpResult;
import cn.jeremy.hadoop.stockcount.hadoop.HdfsFileSystemUtil;
import java.io.IOException;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.httpclient.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * stock数据处理service
 *
 * @author fengjiangtao
 * @date 2020/3/15 22:27
 */
@Service
@Slf4j
public class StockDataService
{
    @Value("${file.download.basepath}")
    private String basePath;

    @Value("${hadoop.hdfs-base-dir}")
    private String hdfsBaseDir;

    @Value("${data.stock.download.url}")
    private String downLoadUrl;

    @Value("${data.stock.upload.url}")
    private String uploadUrl;

    @Autowired
    HdfsFileSystemUtil hdfsFileSystemUtil;

    @Autowired
    CmdService cmdService;

    /**
     * 定时任务执行，下载原始数据，分析数据，上传分析数据
     *
     * @author fengjiangtao
     * @date 2020/3/16 20:32
     */
    @Scheduled(cron = "0 31 12,16 ? * MON-FRI")
    public void stockData()
        throws ZipException
    {
        log.info("start stockData()");
        //下载原始数据
        downlodaRawStockData();
        //上传原始数据到hdfs
        copyStockRawFileToHdfs();
        //统计当天妖股
        cmdService.execDemonStockCount();
        //统计当天连续涨停股票
        cmdService.execContinuousTradingStockCount();
        //统计当天连续跌停股票
        cmdService.execContinuousLimitStockCount();
        //统计股票资金情况
        cmdService.execContinuousStockFundCount();
        //统计股票资金连续流入的股票
        cmdService.execContinuousStockFundUpCount();
        //下载hdfs数据到本地
        copyHdfsStockDataToLocal();
        //上传股票分析数据到外网服务器
        uploadStockAnalyseData();

        log.info("end stockData()");

    }

    public void downlodaRawStockData()
        throws ZipException
    {
        String url = downLoadUrl.replace("{fileName}", "raw");
        String filePath = basePath.concat("raw.zip");
        HttpResult httpResult = HttpTools.getInstance().downFile(url, filePath, "utf-8");
        if (httpResult.getRespCode() == HttpStatus.SC_OK)
        {
            ZipUtil.unZipFolder(filePath, true);
        }
    }

    public void uploadStockAnalyseData()
        throws ZipException
    {
        String date = DateTools.date2TimeStr(new Date(), DateTools.DATE_FORMAT_10);
        uploadStockAnalyseData(date);
    }

    public void uploadStockAnalyseData(String date)
        throws ZipException
    {
        String filePath = basePath.concat(date);
        if (!FileUtil.isFileExists(filePath))
        {
            return;
        }
        ZipUtil.zipFolder(filePath);
        HttpTools.getInstance().postFile(uploadUrl, filePath.concat(".zip"), "utf-8");
        FileUtil.deleteFile(filePath.concat(".zip"));
    }

    /**
     * 将本地的股票数据文件copy到hdfs系统中
     *
     * @author fengjiangtao
     */
    public void copyStockRawFileToHdfs()
    {
        String dir = hdfsBaseDir.concat("raw");
        try
        {
            if (hdfsFileSystemUtil.isExist(dir))
            {
                hdfsFileSystemUtil.delDir(dir);
            }
            hdfsFileSystemUtil.uploadFile(basePath.concat("raw"), dir);
        }
        catch (IOException e)
        {
            //
        }
    }

    /**
     * 从hdfs下载股票分析数据
     *
     * @author fengjiangtao
     */
    public void copyHdfsStockDataToLocal()
    {
        copyHdfsStockDataToLocal(DateTools.date2TimeStr(new Date(), DateTools.DATE_FORMAT_10));
    }

    /**
     * 从hdfs下载股票分析数据
     *
     * @author fengjiangtao
     */
    public void copyHdfsStockDataToLocal(String date)
    {
        try
        {
            if (FileUtil.isFileExists(basePath.concat(date)))
            {
                log.info("delete dir:{}", basePath.concat(date));
                FileUtil.deleteDir(basePath.concat(date));
            }
            hdfsFileSystemUtil.downloadFile(hdfsBaseDir.concat(date), basePath);
        }
        catch (IOException e)
        {
            log.error("copyHdfsStockDataToLocal has error ,e:{}", e);
        }
    }

}
