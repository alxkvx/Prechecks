import os
if os.system("rpm -q python-psycopg2") == '1':  os.system("yum install -y python-psycopg2")

import sys, re, collections, psycopg2, codecs


with open('/usr/local/bm/etc/ssm.conf.d/global.conf', 'r') as myfile:
        data=myfile.read().replace('\n', '')

matchObj = re.match(r'.*DB_HOST = (.*?)DB_USER = (.*?)DB.*', data, re.M|re.I)
pbadb = matchObj.group(1)
pbauser = matchObj.group(2)

logfile = os.path.abspath(os.path.dirname(__file__)) + '/ext_precheck_BA.txt'
log = codecs.open(logfile, encoding='utf-8', mode='w+')

con = psycopg2.connect( user=pbauser, database='pba', host=pbadb )
cur = con.cursor()

cur.execute("SELECT pg_size_pretty(pg_database_size('pba'))")
line  = "\nBA DB size: %s (If size > 15GB: add additional 1H for every 10GB)\n\nShow most fragmented tables and last vacuum time:" % cur.fetchone()[0]
log.write("%s\n" % line)
print line
cur.execute("select a.relname,n_dead_tup, to_char(last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, pg_size_pretty(pg_table_size(b.oid)) as size from pg_stat_all_tables a, pg_class b where a.relname = b.relname order by n_dead_tup desc limit 10")
line = "------------------------+---------------+-----------------------+-----------------------+---------\n relname\t\t| n_dead_tup\t| last_vacuum\t\t| last_autovacuum\t| size\n------------------------+---------------+-----------------------+-----------------------+---------"
log.write("%s\n" % line)
print line
for row in cur.fetchall():
        tab1 = tab2 = tab3 = ''
        if      len(str(row[0])) < 7: tab1 = '\t\t'
        elif    len(str(row[0])) < 15: tab1 = '\t'
        if      len(str(row[2])) < 10: tab2 = '\t\t'
        if      len(str(row[3])) < 10: tab3 = '\t\t'
        line = " %s%s\t| %s\t\t| %s%s\t| %s%s\t| %s " % (row[0],tab1, row[1], row[2],tab2, row[3],tab3, row[4])
        log.write("%s\n" % line)
        print line

print "\nlog saved to: %s\n" % logfile
log.close()