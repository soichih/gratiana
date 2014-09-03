SELECT
    VO.VOName as name,
    sum(R.Cores*R.WallDuration) as wall_sec,
    sum(R.CpuUserDuration) as user_sec,
    sum(R.CpuSystemDuration) as system_sec,
    sum(R.Njobs) as jobs
FROM
    MasterSummaryData R
    JOIN VONameCorrection VC ON (VC.corrid=R.VOcorrid)
    JOIN VO on (VC.void = VO.void)
WHERE R.EndTime = '{endtime}' 
    #some records post abnormally large cpu hours?
    #and R.WallDuration < 100000
    #and R.CpuUserDuration < 100000
    #and R.CpuSystemDuration < 100000
GROUP BY VO.void

