package cn.jeremy.hadoop.stockcount.controller;

import cn.jeremy.hadoop.stockcount.service.CmdService;
import cn.jeremy.hadoop.stockcount.service.StockDataService;
import net.lingala.zip4j.exception.ZipException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestController
{
    @Autowired
    StockDataService stockDataService;

    @Autowired
    CmdService cmdService;

    @RequestMapping(value = "/download", method = RequestMethod.GET)
    public String download()
        throws ZipException
    {
        stockDataService.downlodaRawStockData();
        return "success";
    }

    @RequestMapping(value = "/upload/{date}", method = RequestMethod.GET)
    public String upload(@PathVariable String date)
        throws ZipException
    {
        stockDataService.uploadStockAnalyseData(date);
        return "success";
    }

    @RequestMapping(value = "/uploadHdfs", method = RequestMethod.GET)
    public String uploadHdfs()
        throws ZipException
    {
        stockDataService.copyStockRawFileToHdfs();
        return "success";
    }

    @RequestMapping(value = "/downFromHdfs/{date}", method = RequestMethod.GET)
    public String downFromHdfs(@PathVariable String date)
        throws ZipException
    {
        stockDataService.copyHdfsStockDataToLocal(date);
        return "success";
    }

    @RequestMapping(value = "/execDemonStockCount/{date}", method = RequestMethod.GET)
    public String execDemonStockCount(@PathVariable String date)
        throws ZipException
    {
        cmdService.execDemonStockCount(date);
        return "success";
    }

    @RequestMapping(value = "/stockData", method = RequestMethod.GET)
    public String stockData()
        throws ZipException
    {
        stockDataService.stockData();
        return "success";
    }

}
