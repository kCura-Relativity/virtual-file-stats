/* Run this all at once, produces two result sets. */

/* 
Storage throughput by database file.
These numbers are an average since startup.
Adapted From  David Pless: https://blogs.msdn.com/b/dpless/archive/2010/12/01/leveraging-sys-dm-io-virtual-file-stats.aspx

Thresholds taken from SQLCAT whitepaper, page 7: http://msdn.microsoft.com/en-us/library/ee410782.aspx
*/
SELECT  @@SERVERNAME ServerName, DB_NAME(a.database_id) AS [db_name] ,
        b.name + N' [' + b.type_desc COLLATE SQL_Latin1_General_CP1_CI_AS + N']' AS file_logical_name ,
        UPPER(SUBSTRING(b.physical_name, 1, 2)) AS disk_location ,
        CAST(( ( a.size_on_disk_bytes / 1024.0 ) / 1024.0 ) AS INT) AS size_on_disk_mb ,
        a.io_stall_read_ms ,
        a.num_of_reads ,
        CASE WHEN a.num_of_bytes_read > 0 
            THEN CAST(a.num_of_bytes_read/1024.0/1024.0/1024.0 AS NUMERIC(23,1))
            ELSE 0 
        END AS GB_read,
        CAST(a.io_stall_read_ms / ( 1.0 * a.num_of_reads ) AS INT) AS avg_read_stall_ms ,
        avg_read_stall_ms_recommended_max = CASE 
            WHEN b.type = 0 THEN 30 /* data files */
            WHEN b.type = 1 THEN 5 /* log files */
            ELSE 0
        END ,
        a.io_stall_write_ms ,
        a.num_of_writes ,
        CASE WHEN a.num_of_bytes_written > 0 
            THEN CAST(a.num_of_bytes_written/1024.0/1024.0/1024.0 AS NUMERIC(23,1))
            ELSE 0 
        END AS GB_written,
        CAST(a.io_stall_write_ms / ( 1.0 * a.num_of_writes ) AS INT) AS avg_write_stall_ms ,
        avg_write_stall_ms_recommended_max = CASE 
            WHEN b.type = 0 THEN 30 /* data files */
            WHEN b.type = 1 THEN 2 /* log files */
            ELSE 0
        END ,
        b.physical_name,
        related_wait_type_reads = CASE
            WHEN b.name = 'tempdb' THEN 'N/A'
            WHEN b.type = 1 THEN 'N/A' /* log files */
            ELSE 'PAGEIOLATCH*'
        END ,
        related_wait_type_writes = CASE
            WHEN b.type = 1 THEN 'WRITELOG' /* log files */
            WHEN b.name = 'tempdb' THEN 'xxx' /* tempdb data files */
            WHEN b.type = 0 THEN 'ASYNC_IO_COMPLETION' /* data files */
            ELSE 'xxx'
        END,
        GETDATE() AS sample_time      
FROM    sys.dm_io_virtual_file_stats(NULL, NULL) AS a
        INNER JOIN sys.master_files AS b ON a.file_id = b.file_id
                                            AND a.database_id = b.database_id
WHERE   a.num_of_reads > 0
        AND a.num_of_writes > 0
ORDER BY avg_read_stall_ms DESC;
GO


/*
Storage throughput by drive letter
These numbers are an average since startup.
Note: mount points are not broken out separately
Adapted From  David Pless: https://blogs.msdn.com/b/dpless/archive/2010/12/01/leveraging-sys-dm-io-virtual-file-stats.aspx?Redirected=true
*/
SELECT  @@SERVERNAME SQLServer, UPPER(SUBSTRING(b.physical_name, 1, 2)) AS disk_location ,
        SUM(a.io_stall_read_ms) AS io_stall_read_ms ,
        SUM(a.num_of_reads) AS num_of_reads ,
        CAST(SUM(a.num_of_bytes_read)/1024.0/1024.0/1024.0 AS NUMERIC(23,1)) AS GB_read,
        CASE WHEN SUM(a.num_of_reads) > 0
          THEN CAST(SUM(a.io_stall_read_ms) / ( 1.0 * SUM(a.num_of_reads) ) AS INT) 
          ELSE CAST(0 AS INT) END AS avg_read_stall_ms ,
        SUM(a.io_stall_write_ms) AS io_stall_write_ms ,
        SUM(a.num_of_writes) AS num_of_writes ,
        CAST(SUM(a.num_of_bytes_written)/1024.0/1024.0/1024.0 AS NUMERIC(23,1)) AS GB_written,
        CASE WHEN SUM(a.num_of_writes) > 0
          THEN CAST(SUM(a.io_stall_write_ms) / ( 1.0 * SUM(a.num_of_writes) ) AS INT) 
          ELSE CAST(0 AS INT) END AS avg_writes_stall_ms
FROM    sys.dm_io_virtual_file_stats(NULL, NULL) a
        INNER JOIN sys.master_files b ON a.file_id = b.file_id
                                         AND a.database_id = b.database_id
GROUP BY UPPER(SUBSTRING(b.physical_name, 1, 2))
ORDER BY 4 DESC;
GO