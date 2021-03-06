package org.example.job.admin.conf;

import org.example.job.admin.dao.*;
import org.example.job.admin.scheduler.JobScheduler;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.Arrays;

@Component
public class JobAdminConfig implements InitializingBean, DisposableBean {

    private static JobAdminConfig adminConfig = null;

    public static JobAdminConfig getAdminConfig() {
        return adminConfig;
    }

    // ---------------------- XxlJobScheduler ----------------------

    private JobScheduler jobScheduler;

    @Override
    public void afterPropertiesSet() throws Exception {
        adminConfig = this;

        jobScheduler = new JobScheduler();
        jobScheduler.init();
    }

    @Override
    public void destroy() throws Exception {

    }
    // ---------------------- XxlJobScheduler ----------------------

    // conf
    @Value("${job.i18n}")
    private String i18n;

    @Value("${job.accessToken}")
    private String accessToken;

    @Value("${spring.mail.from}")
    private String emailFrom;

    @Value("${job.triggerpool.fast.max}")
    private int triggerPoolFastMax;

    @Value("${job.triggerpool.slow.max}")
    private int triggerPoolSlowMax;

    @Value("${job.logretentiondays}")
    private int logretentiondays;

    // dao, service

    @Resource
    private JobLogDao jobLogDao;
    @Resource
    private JobInfoDao jobInfoDao;
    @Resource
    private JobRegistryDao jobRegistryDao;
    @Resource
    private JobGroupDao jobGroupDao;
    @Resource
    private JobLogReportDao jobLogReportDao;
    @Resource
    private DataSource dataSource;

//    @Resource
//    private JavaMailSender mailSender;
//    @Resource
//    private JobAlarmer jobAlarmer;

    public String getI18n() {
        if (!Arrays.asList("zh_CN", "zh_TC", "en").contains(i18n)) {
            return "zh_CN";
        }
        return i18n;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getEmailFrom() {
        return emailFrom;
    }

    public int getTriggerPoolFastMax() {
        if (triggerPoolFastMax < 200) {
            return 200;
        }
        return triggerPoolFastMax;
    }

    public int getTriggerPoolSlowMax() {
        if (triggerPoolSlowMax < 100) {
            return 100;
        }
        return triggerPoolSlowMax;
    }

    public int getLogretentiondays() {
        if (logretentiondays < 7) {
            return -1;  // Limit greater than or equal to 7, otherwise close
        }
        return logretentiondays;
    }

    public JobLogDao getJobLogDao() {
        return jobLogDao;
    }

    public JobInfoDao getJobInfoDao() {
        return jobInfoDao;
    }

    public JobRegistryDao getJobRegistryDao() {
        return jobRegistryDao;
    }

    public JobGroupDao getJobGroupDao() {
        return jobGroupDao;
    }

    public JobLogReportDao getJobLogReportDao() {
        return jobLogReportDao;
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
