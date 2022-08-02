import pandahouse as ph

connection_default = {'host': '',
                      'database': '',
                      'user': '', 
                      'password': ''
                      }

metrics_sql = '''
SELECT test_grp,
       sum(money)/uniqExact(st_id) AS ARPU,
       sum(money)/uniqExactIf(st_id, active = 1) AS ARPPU,
       uniqExactIf(st_id, buyer = 1)/uniqExact(st_id) AS CR,
       uniqExactIf(st_id, active = 1
                   AND buyer = 1)/uniqExact(st_id) AS active_CR,
       uniqExactIf(st_id, active = 1
                   AND buyer = 1
                   AND buy = 'Math')/uniqExactIf(st_id, buy = 'Math') AS math_CR
FROM
  (SELECT st_id,
          test_grp,
          buy,
          active,
          money,
          if(money > 0, 1, 0) AS buyer
   FROM
     (SELECT DISTINCT *
      FROM default.studs AS studs) AS studs
   LEFT OUTER JOIN
     (SELECT st_id,
             subject AS buy,
             if(time < sale_time, 1, 0) AS active,
             money
      FROM
        (SELECT st_id,
                subject,
                min(timest) AS time
         FROM default.peas
         GROUP BY st_id,
                  subject) AS t1
      FULL OUTER JOIN
        (SELECT st_id,
                subject,
                min(sale_time) AS sale_time,
                sum(money) AS money
         FROM default.final_project_check
         GROUP BY st_id,
                  subject) AS t2 ON t1.st_id = t2.st_id
      AND t1.subject = t2.subject) AS t2 ON studs.st_id = t2.st_id) AS t1
GROUP BY test_grp
'''

metrics_query = ph.read_clickhouse(query=metrics_sql, connection=connection_default)
metrics_query
