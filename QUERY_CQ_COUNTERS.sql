--CQ counters
select * from ( 
	select DATE_ADD( '1970-01-01', 18904) as day_time,LAST_SAI_CGI_ECGI, Direction,	
						SUM(CASE WHEN CQ=='E' THEN 1 ELSE 0 END) AS Excellent_p, 
						SUM(CASE WHEN CQ=='E' OR CQ=='C' THEN 1 ELSE 0 END) AS Core_p,
						SUM(CASE WHEN CQ=='NM' THEN 1 ELSE 0 END) AS NotMet_p,
						count(*) as record_qty
	from ( 
		select *, 	CASE 	WHEN Direction='Downlink' AND ((Throughput_cat='Ex' AND RTT_ms<150 AND Packet_Loss_Rate<1) OR (Throughput_cat='Ex' AND Jitter_ms<30)) THEN 'E'
							WHEN Direction='Downlink' AND (((Throughput_cat='Ex' OR Throughput_cat='Co') AND RTT_ms<250 AND Packet_Loss_Rate<5) OR ((Throughput_cat='Ex' OR Throughput_cat='Co') AND Jitter_ms<50)) THEN 'C'
							ELSE 'NM' END
					AS CQ
		from ( 
		
			select *, CASE 	WHEN 	(Througput_Mbps>=0.25 AND Througput_Mbps<=2 AND Traffic_MB>=0.5 AND Traffic_MB<1) OR
									(Througput_Mbps>=0.25 AND Througput_Mbps<=2.5 AND Traffic_MB>=1 AND Traffic_MB<2.5) OR
									(Througput_Mbps>=0.5 AND Througput_Mbps<=3 AND Traffic_MB>=2.5 AND Traffic_MB<5) OR
									(Througput_Mbps>=0.75 AND Througput_Mbps<=3 AND Traffic_MB>=5 AND Traffic_MB<7.5) OR
									(Througput_Mbps>=1 AND Througput_Mbps<=3.5 AND Traffic_MB>=7.5 AND Traffic_MB<10) OR
									(Througput_Mbps>=1 AND Througput_Mbps<=3.5 AND Traffic_MB>=10 AND Traffic_MB<20) OR
									(Througput_Mbps>=1 AND Througput_Mbps<=3.5 AND Traffic_MB>=20 AND Traffic_MB<50) OR
									(Througput_Mbps>=1.5 AND Througput_Mbps<=4 AND Traffic_MB>=50 AND Traffic_MB<100) OR
									(Througput_Mbps>=1.5 AND Througput_Mbps<=4.5 AND Traffic_MB>=100 AND Traffic_MB<200) OR
									(Througput_Mbps>=1.5 AND Througput_Mbps<=5 AND Traffic_MB>=200) THEN 'Co'
							WHEN	(Througput_Mbps>=2 AND Traffic_MB>=0.5 AND Traffic_MB<1) OR
									(Througput_Mbps>=2.5 AND Traffic_MB>=1 AND Traffic_MB<2.5) OR
									(Througput_Mbps>=3 AND Traffic_MB>=2.5 AND Traffic_MB<5) OR
									(Througput_Mbps>=3 AND Traffic_MB>=5 AND Traffic_MB<7.5) OR
									(Througput_Mbps>=3.5 AND Traffic_MB>=7.5 AND Traffic_MB<10) OR
									(Througput_Mbps>=3.5 AND Traffic_MB>=10 AND Traffic_MB<20) OR
									(Througput_Mbps>=3.5 AND Traffic_MB>=20 AND Traffic_MB<50) OR
									(Througput_Mbps>=4 AND Traffic_MB>=50 AND Traffic_MB<100) OR
									(Througput_Mbps>=4.5 AND Traffic_MB>=100 AND Traffic_MB<200) OR
									(Througput_Mbps>=5 AND Traffic_MB>=200) THEN 'Ex'
							ELSE 'Nmm' END AS  Throughput_cat
			from (  
				--Streaming DL
				select   'Downlink' as Direction,LAST_SAI_CGI_ECGI,
						  L7_DL_GOODPUT_FULL_MSS/1024/1024 AS Traffic_MB,
						 (L7_DL_GOODPUT_FULL_MSS*8/(DATATRANS_DL_DURATION/1000))/1024/1024 as Througput_Mbps,
						 (USER_PROBE_DW_LOST_PKT+SERVER_PROBE_DW_LOST_PKT)/TCP_DW_PACKAGES_WITHPL*100 as Packet_Loss_Rate,
						 AVG_DW_RTT as RTT_ms,
						 AVG_DL_JITTER as Jitter_ms
				from ps.detail_ufdr_streaming_18904
				where L7_DL_GOODPUT_FULL_MSS/1024>=500 AND DATATRANS_DL_DURATION/1000>1 AND DATATRANS_DL_DURATION/1000<3600 AND LAST_SAI_CGI_ECGI like '73009%' AND PROT_CATEGORY in (3,4,6,10)
				--AND IMSI like '730090028821893'
				union all
				--HTTP Browsing DL
				select   'Downlink' as Direction,LAST_SAI_CGI_ECGI,
						  L7_DL_GOODPUT_FULL_MSS/1024/1024 AS Traffic_MB,
						 (L7_DL_GOODPUT_FULL_MSS*8/(DATATRANS_DW_DURATION/1000))/1024/1024 as Througput_Mbps,
						 (USER_PROBE_DW_LOST_PKT+SERVER_PROBE_DW_LOST_PKT)/TCP_DW_PACKAGES_WITHPL*100 as Packet_Loss_Rate,
						 AVG_DW_RTT as RTT_ms,
						 AVG_DL_JITTER as Jitter_ms
				from ps.detail_ufdr_http_browsing_18904
				where L7_DL_GOODPUT_FULL_MSS/1024>=500 AND DATATRANS_DW_DURATION/1000>1 AND DATATRANS_DW_DURATION/1000<3600 AND LAST_SAI_CGI_ECGI like '73009%' AND PROT_CATEGORY in (3,4,6,10)
				--AND IMSI like '730090028821893'
				union all
				--Other DL
				select   'Downlink' as Direction,LAST_SAI_CGI_ECGI,
						  L7_DW_GOODPUT_FULL_MSS/1024/1024 AS Traffic_MB,
						 (L7_DW_GOODPUT_FULL_MSS*8/(DATATRANS_DW_DURATION/1000000000))/1024/1024 as Througput_Mbps,
						 (USER_PROBE_DW_LOST_PKT+SERVER_PROBE_DW_LOST_PKT)/TCP_DW_PACKAGES_WITHPL*100 as Packet_Loss_Rate,
						 AVG_DW_RTT as RTT_ms,
						 AVG_DL_JITTER as Jitter_ms
				from ps.detail_ufdr_other_18904
				where L7_DW_GOODPUT_FULL_MSS/1024>=500 AND DATATRANS_DW_DURATION/1000000000>1 AND DATATRANS_DW_DURATION/1000000000<3600 AND LAST_SAI_CGI_ECGI like '73009%' AND PROT_CATEGORY in (3,4,6,10)
				--AND IMSI like '730090028821893'
				union all
				--Fileaccess DL
				select   'Downlink' as Direction,LAST_SAI_CGI_ECGI,
						  L7_DW_GOODPUT_FULL_MSS/1024/1024 AS Traffic_MB,
						 (L7_DW_GOODPUT_FULL_MSS*8/(DATATRANS_DW_DURATION/1000000000))/1024/1024 as Througput_Mbps,
						 (USER_PROBE_DW_LOST_PKT+SERVER_PROBE_DW_LOST_PKT)/TCP_DW_PACKAGES_WITHPL*100 as Packet_Loss_Rate,
						 AVG_DW_RTT as RTT_ms,
						 AVG_DL_JITTER as Jitter_ms
				from ps.detail_ufdr_fileaccess_18904
				where L7_DW_GOODPUT_FULL_MSS/1024>=500 AND DATATRANS_DW_DURATION/1000000000>1 AND DATATRANS_DW_DURATION/1000000000<3600 AND LAST_SAI_CGI_ECGI like '73009%' AND PROT_CATEGORY in (3,4,6,10)
				--AND IMSI like '730090028821893'
			) Z
		) A
	) B
	group by LAST_SAI_CGI_ECGI, Direction
) C
join (select CELL_NAME, CGISAI, ACCESS_TYPE, XPOS/1000000 as longitude, YPOS/1000000 as latitude, layer2name,layer4name from nethouse.dim_loc_cgisai) D
on C.LAST_SAI_CGI_ECGI=D.CGISAI




