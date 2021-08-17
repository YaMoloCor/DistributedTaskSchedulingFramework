package org.example.job.admin.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.example.job.admin.model.JobLogReport;

import java.util.Date;
import java.util.List;


@Mapper
public interface JobLogReportDao {

    int save(JobLogReport xxlJobLogReport);

    int update(JobLogReport xxlJobLogReport);

    List<JobLogReport> queryLogReport(@Param("triggerDayFrom") Date triggerDayFrom,
                                      @Param("triggerDayTo") Date triggerDayTo);

    JobLogReport queryLogReportTotal();

}
