SELECT
    P.ProjectName as name,
    sum(M.Cores*M.WallDuration) as wall_sec,
    sum(M.CpuUserDuration) as user_sec,
    sum(M.CpuSystemDuration) as system_sec,
    sum(M.Njobs) as jobs 
FROM
    MasterSummaryData M
    JOIN ProjectNameCorrection P on M.ProjectNameCorrid = P.ProjectNameCorrid
WHERE M.EndTime = '{endtime}'
GROUP BY P.ProjectNameCorrid

