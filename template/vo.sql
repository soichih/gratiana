select
    LOWER(ReportableVOName) as name,
    sum(WallDuration * IFNULL(Processors,1) ) as wall_sec,
    sum(CpuUserDuration * IFNULL(Processors,1) ) as user_sec,
    sum(CpuSystemDuration * IFNULL(Processors,1) ) as system_sec,
    sum(Njobs) as jobs
from
    JobUsageRecord
where EndTime >= '{starttime}' and EndTime < '{endtime}' 
    # limiter for strangely large cpu hours
    #and WallDuration < 100000
    #and CpuUserDuration < 100000
    #and CpuSystemDuration < 100000
group by LOWER(ReportableVOName)
