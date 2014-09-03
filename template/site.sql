select
    S.SiteName as name,
    sum(WallDuration * IFNULL(Processors,1) ) as wall_sec,
    sum(CpuUserDuration * IFNULL(Processors,1) ) as user_sec,
    sum(CpuSystemDuration * IFNULL(Processors,1) ) as system_sec,
    sum(Njobs) as jobs
from JobUsageRecord J
join JobUsageRecord_Meta M on J.dbid = M.dbid
join Probe P on M.ProbeName = P.probename
join Site S on P.siteid = S.siteid
where EndTime >= '{starttime}' and EndTime < '{endtime}'
    #and WallDuration < 100000
    #and CpuUserDuration < 100000
    #and CpuSystemDuration < 100000
group by S.siteid
