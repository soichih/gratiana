SELECT
    S.SiteName as name,
    sum(R.Cores*R.WallDuration) as wall_sec,
    sum(R.CpuUserDuration) as user_sec,
    sum(R.CpuSystemDuration) as system_sec,
    sum(R.Njobs) as jobs 
FROM
    MasterSummaryData R
    JOIN Probe P on R.ProbeName = P.probename
    JOIN Site S on S.siteid = P.siteid
WHERE R.EndTime = '{endtime}'
GROUP BY S.siteid

