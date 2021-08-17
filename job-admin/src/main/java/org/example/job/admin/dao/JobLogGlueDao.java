package org.example.job.admin.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.example.job.admin.model.JobLogGlue;

import java.util.List;


@Mapper
public interface JobLogGlueDao {

    int save(JobLogGlue xxlJobLogGlue);

    List<JobLogGlue> findByJobId(@Param("jobId") int jobId);

    int removeOld(@Param("jobId") int jobId, @Param("limit") int limit);

    int deleteByJobId(@Param("jobId") int jobId);

}
