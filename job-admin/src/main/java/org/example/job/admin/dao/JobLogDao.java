package org.example.job.admin.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.example.job.admin.model.JobLog;

import java.util.Date;
import java.util.List;
import java.util.Map;


@Mapper
public interface JobLogDao {

	// exist jobId not use jobGroup, not exist use jobGroup
	 List<JobLog> pageList(@Param("offset") int offset,
								 @Param("pagesize") int pagesize,
								 @Param("jobGroup") int jobGroup,
								 @Param("jobId") int jobId,
								 @Param("triggerTimeStart") Date triggerTimeStart,
								 @Param("triggerTimeEnd") Date triggerTimeEnd,
								 @Param("logStatus") int logStatus);
	 int pageListCount(@Param("offset") int offset,
							 @Param("pagesize") int pagesize,
							 @Param("jobGroup") int jobGroup,
							 @Param("jobId") int jobId,
							 @Param("triggerTimeStart") Date triggerTimeStart,
							 @Param("triggerTimeEnd") Date triggerTimeEnd,
							 @Param("logStatus") int logStatus);
	
	 JobLog load(@Param("id") long id);

	 long save(JobLog jobLog);

	 int updateTriggerInfo(JobLog jobLog);

	 int updateHandleInfo(JobLog jobLog);
	
	 int delete(@Param("jobId") int jobId);

	 Map<String, Object> findLogReport(@Param("from") Date from,
											 @Param("to") Date to);

	 List<Long> findClearLogIds(@Param("jobGroup") int jobGroup,
									  @Param("jobId") int jobId,
									  @Param("clearBeforeTime") Date clearBeforeTime,
									  @Param("clearBeforeNum") int clearBeforeNum,
									  @Param("pagesize") int pagesize);
	 int clearLog(@Param("logIds") List<Long> logIds);

	 List<Long> findFailJobLogIds(@Param("pagesize") int pagesize);

	 int updateAlarmStatus(@Param("logId") long logId,
								 @Param("oldAlarmStatus") int oldAlarmStatus,
								 @Param("newAlarmStatus") int newAlarmStatus);

	 List<Long> findLostJobIds(@Param("losedTime") Date losedTime);

}
