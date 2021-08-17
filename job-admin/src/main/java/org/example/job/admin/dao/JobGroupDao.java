package org.example.job.admin.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.example.job.admin.model.JobGroup;

import java.util.List;

/**
 * Created by xuxueli on 16/9/30.
 */
@Mapper
public interface JobGroupDao {

     List<JobGroup> findAll();

     List<JobGroup> findByAddressType(@Param("addressType") int addressType);

     int save(JobGroup xxlJobGroup);

     int update(JobGroup xxlJobGroup);

     int remove(@Param("id") int id);

     JobGroup load(@Param("id") int id);

     List<JobGroup> pageList(@Param("offset") int offset,
                                      @Param("pagesize") int pagesize,
                                      @Param("appname") String appname,
                                      @Param("title") String title);

     int pageListCount(@Param("offset") int offset,
                             @Param("pagesize") int pagesize,
                             @Param("appname") String appname,
                             @Param("title") String title);

}
