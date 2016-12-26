package com.ifeng.logcollector;


import com.ifeng.configurable.ComponentConfiguration;
import com.ifeng.configurable.Configurable;
import com.ifeng.configurable.Context;
import com.ifeng.logcollector.sources.ExecSource;
import com.ifeng.logcollector.sources.SpoolDirSource;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zhanglr on 2016/9/22.
 */
public class Server {
    private static Logger logger = Logger.getLogger(Server.class);
    public static void main(String[] args) {

        ExecutorService executorService = null;
        try {
            if (args.length == 0) {
                    System.err.println("args can not be empty!");
                    return;
                }
                String basePath = args[0];
                PropertyConfigurator.configure(basePath+"/log4j.properties");

                ComponentConfiguration configuration = new ComponentConfiguration(basePath+"/collector.properties");
                HashMap<String, Context> map = configuration.load();
                executorService = Executors.newFixedThreadPool(map.size());
                List<String> sources = configuration.getAllInstances();
                for (String source : sources){
                    Context context = map.get(source);

                    if (context.getString("type").toLowerCase().equals("exec")) {
                        Configurable work = new ExecSource();
                        map.get(source).put("basePath",basePath);
                        work.config(map.get(source));
                        executorService.execute((Runnable) work);
                    }else{
                        SpoolDirSource s = new SpoolDirSource();
                        map.get(source).put("basePath",basePath);
                        s.config(map.get(source));
                        s.start();
                    }
                }
            }catch (Exception e){
                if (executorService != null && !executorService.isShutdown()){
                    executorService.shutdown();
            }
            logger.error(e);
        }
    }
}