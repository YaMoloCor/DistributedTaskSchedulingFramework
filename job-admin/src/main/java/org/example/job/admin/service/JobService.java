package org.example.job.admin.service;

import org.example.job.admin.model.JobInfo;
import org.example.job.core.biz.model.ReturnT;

import java.util.Date;
import java.util.Map;

public interface JobService {
    Map<String, Object> dashboardInfo();

    ReturnT<Map<String, Object>> chartInfo(Date startDate, Date endDate);

    Map<String, Object> pageList(int start, int length, int jobGroup, int triggerStatus,
                                 String jobDesc, String executorHandler, String author);

    ReturnT<String> add(JobInfo jobInfo);

    ReturnT<String> update(JobInfo jobInfo);

    ReturnT<String> remove(int id);

    ReturnT<String> stop(int id);

    ReturnT<String> start(int id);
}
