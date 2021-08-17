package org.example.job.admin.thread;

import org.example.job.admin.conf.JobAdminConfig;
import org.example.job.admin.model.JobGroup;
import org.example.job.admin.model.JobRegistry;
import org.example.job.core.biz.AdminBiz;
import org.example.job.core.biz.model.HandleCallbackParam;
import org.example.job.core.biz.model.RegistryParam;
import org.example.job.core.biz.model.ReturnT;
import org.example.job.core.enums.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class JobRegistryHelper {
    private static Logger logger = LoggerFactory.getLogger(JobRegistryHelper.class);
    private static JobRegistryHelper instance = new JobRegistryHelper();

    public static JobRegistryHelper getInstance() {
        return instance;
    }

    private ThreadPoolExecutor registryOrRemoveThreadPool = null;
    private Thread registryMonitorThread = null;
    private volatile boolean toStop = false;

    public void start() {
        registryOrRemoveThreadPool = new ThreadPoolExecutor(2, 10, 30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(2000),
                r -> new Thread(r, "job admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-" + r.hashCode()),
                (r, executor) -> {
                    r.run();
                    logger.warn(">>>>>>>>>>> job, registry or remove too fast, match threadpool rejected handler(run now).");
                }
        );
        registryMonitorThread = new Thread(() -> {
            while (!toStop) {
                try {
                    // auto registry group
                    List<JobGroup> jobGroupList = JobAdminConfig.getAdminConfig().getJobGroupDao().findByAddressType(0);
                    if (jobGroupList != null && !jobGroupList.isEmpty()) {
                        // remove dead address (admin/executor)
                        List<Integer> deadGroupIds = JobAdminConfig.getAdminConfig().getJobRegistryDao().findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
                        if (deadGroupIds != null && !deadGroupIds.isEmpty()) {
                            JobAdminConfig.getAdminConfig().getJobRegistryDao().removeDead(deadGroupIds);
                        }
                        // fresh online address (admin/executor)
                        Map<String, List<String>> addressMap = new HashMap<>();
                        List<JobRegistry> jobRegistryList = JobAdminConfig.getAdminConfig().getJobRegistryDao().findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
                        if (jobRegistryList != null && !jobRegistryList.isEmpty()) {
                            for (JobRegistry item : jobRegistryList) {
                                if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
                                    String appName = item.getRegistryKey();
                                    List<String> registryList = addressMap.get(appName);
                                    if (registryList == null) {
                                        registryList = new ArrayList<>();
                                    }
                                    if (!registryList.contains(item.getRegistryValue())) {
                                        registryList.add(item.getRegistryValue());
                                    }
                                    addressMap.put(appName, registryList);
                                }
                            }
                        }
                        // fresh group address
                        for (JobGroup item : jobGroupList) {
                            List<String> registryList = addressMap.get(item.getAppname());
                            String addressListStr = null;
                            if (registryList != null && !registryList.isEmpty()) {
                                Collections.sort(registryList);
                                StringBuilder sb = new StringBuilder();
                                for (String str : registryList) {
                                    sb.append(str).append(",");
                                }
                                addressListStr = sb.toString();
                                addressListStr.substring(0, addressListStr.length() - 1);
                            }
                            item.setAddressList(addressListStr);
                            item.setUpdateTime(new Date());
                            JobAdminConfig.getAdminConfig().getJobGroupDao().update(item);
                        }
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> job, job registry monitor thread error:{}", e);
                    }
                }

                try {
                    TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);//睡眠30s
                } catch (InterruptedException e) {
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> job, job registry monitor thread error:{}", e);
                    }
                }
            }
            logger.info(">>>>>>>>>>> job, job registry monitor thread stop");
        });

        registryMonitorThread.setDaemon(true);
        registryMonitorThread.setName("job, admin JobRegistryMonitorHelper-registryMonitorThread");
        registryMonitorThread.start();
    }

    public void stop() {
        toStop = true;    // stop registryOrRemoveThreadPool
        registryOrRemoveThreadPool.shutdownNow();

        // stop monitir (interrupt and wait)
        registryMonitorThread.interrupt();
        try {
            registryMonitorThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    // ---------------------- helper ----------------------
    public ReturnT<String> registry(RegistryParam registryParam) {
        // valid
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }
        // async execute
        registryOrRemoveThreadPool.execute(() -> {
            int ret = JobAdminConfig.getAdminConfig().getJobRegistryDao().registryUpdate(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
                    registryParam.getRegistryValue(), new Date());
            if (ret < 1) {
                JobAdminConfig.getAdminConfig().getJobRegistryDao().registrySave(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
                        registryParam.getRegistryValue(), new Date());
                // fresh
                freshGroupRegistryInfo(registryParam);
            }
        });
        return ReturnT.SUCCESS;
    }

    private void freshGroupRegistryInfo(RegistryParam registryParam) {
        // TODO
    }

    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        // valid
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }
        registryOrRemoveThreadPool.execute(() -> {
            int ret = JobAdminConfig.getAdminConfig().getJobRegistryDao().registryDelete(registryParam.getRegistryGroup(),
                    registryParam.getRegistryKey(),
                    registryParam.getRegistryValue());
            if (ret > 0) {
                freshGroupRegistryInfo(registryParam);
            }
        });
        return ReturnT.SUCCESS;
    }

}
