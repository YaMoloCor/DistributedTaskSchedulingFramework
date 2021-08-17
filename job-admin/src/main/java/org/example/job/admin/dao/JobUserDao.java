package org.example.job.admin.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.example.job.admin.model.JobUser;

import java.util.List;

/**
 * @author xuxueli 2019-05-04 16:44:59
 */
@Mapper
public interface JobUserDao {

    List<JobUser> pageList(@Param("offset") int offset,
                           @Param("pagesize") int pagesize,
                           @Param("username") String username,
                           @Param("role") int role);

    int pageListCount(@Param("offset") int offset,
                      @Param("pagesize") int pagesize,
                      @Param("username") String username,
                      @Param("role") int role);

    JobUser loadByUserName(@Param("username") String username);

    int save(JobUser xxlJobUser);

    int update(JobUser xxlJobUser);

    int delete(@Param("id") int id);

}
