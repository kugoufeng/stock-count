package cn.jeremy.hadoop.stockcount.service;

import cn.jeremy.common.utils.DateTools;
import cn.jeremy.common.utils.FileUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 执行cmd命令
 *
 * @author fengjiangtao
 * @date 2020/3/15 23:56
 */
@Component
@Slf4j
public class CmdService
{
    @Value("${mr.app.path}")
    private String mrAppPath;

    @Value("${hadoop.hdfs-base-dir}")
    private String hdfsBaseDir;

    public void execDemonStockCount()
    {
        execDemonStockCount(DateTools.date2TimeStr(new Date(), DateTools.DATE_FORMAT_10));
    }

    public void execDemonStockCount(String date)
    {
        execCountMr("cn.jeremy.hadoop.stockcount.mr.DemonStockCount", date);
    }

    public void execCountMr(String mainClass, String date)
    {
        String[] args = {hdfsBaseDir.concat("raw"), hdfsBaseDir, date};

        execMr(mrAppPath, mainClass, args);
    }

    public void execMr(String jarPath, String mainClass, String[] args)
    {
        String cmd = "hadoop jar " + jarPath + " " + mainClass;
        if (args != null)
        {
            for (String arg : args)
            {
                cmd = cmd.concat(" ").concat(arg);
            }
        }
        log.info("exec cmd:{}", cmd);
        runCMD(cmd);
    }

    public boolean runCMD(String cmd)
    {
        final String methodName = "runCMD";
        BufferedReader br = null;
        try
        {
            Process p = Runtime.getRuntime().exec(cmd);
            br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String readLine = br.readLine();
            StringBuilder builder = new StringBuilder();
            while (readLine != null)
            {
                readLine = br.readLine();
                builder.append(readLine);
            }
            log.debug(methodName + "#readLine: " + builder.toString());

            p.waitFor();
            int i = p.exitValue();
            log.info(methodName + "#exitValue = " + i);
            if (i == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        catch (Exception e)
        {
            log.error(methodName + "#ErrMsg=" + e.getMessage());
        }
        finally
        {
            FileUtil.closeIO(br);
        }
        return false;
    }
}
