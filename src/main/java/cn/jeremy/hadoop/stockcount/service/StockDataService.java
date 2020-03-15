package cn.jeremy.hadoop.stockcount.service;

import cn.jeremy.common.utils.DateTools;
import cn.jeremy.common.utils.FileUtil;
import cn.jeremy.common.utils.HttpTools;
import cn.jeremy.common.utils.ZipUtil;
import cn.jeremy.common.utils.bean.HttpResult;
import cn.jeremy.hadoop.stockcount.hadoop.HdfsFileSystemUtil;
import java.io.IOException;
import java.util.Date;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.httpclient.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * stock数据处理service
 *
 * @author fengjiangtao
 * @date 2020/3/15 22:27
 */
@Service
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

    public void downlodaRawStockData()
        throws ZipException
    {
        String url = downLoadUrl.replace("{fileName}", "raw");
        String filePath = basePath.concat("raw.zip");
        HttpResult httpResult = HttpTools.getInstance().downFile(url, filePath, "utf-8");
        if (httpResult.getRespCode() == HttpStatus.SC_OK)
        {
            ZipUtil.unZipFolder(filePath);
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
     * 将本地的股票数据文件copy到hdfs系统中
     *
     * @author fengjiangtao
     */
    public void copyHdfsStockDataToLocal(String date)
    {
        try
        {
            hdfsFileSystemUtil.downloadFile(hdfsBaseDir.concat(date), basePath);
        }
        catch (IOException e)
        {
            //
        }
    }

}
