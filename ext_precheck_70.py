#!/usr/bin/python

import os, sys, atexit, re, shutil, codecs, deployment, install_routines, optparse, time, logging, subprocess, socket
if not os.path.isfile('poaupdater/uPgBench.py'): os.rename('uPgBench.py','poaupdater/uPgBench.py')
from poaupdater import uConfig, uLogging, uSysDB, uPEM, uPrecheck, uUtil, openapi, uHCL, uBilling, uPgBench

def diskspace():

	logging.info('\n\t============== Checking free space on all nodes (Free space > 1GB) ==============\n')
	
	free_space = 1 # GB
	cur.execute("select host_id, primary_name from hosts where pleskd_id>0 and htype not in ('w','e')")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("Checking free disk space on %s" % name)
		try:
			res = uPEM.check_free_disk_space(host_id, free_space)
			if res is None:
				logging.info("Result:\t[  OK  ]")
		except Exception, e:
			logging.info("%s\n" % str(e))
			continue

def ui_resources():

	logging.info("\n\t============== Checking UI/MN nodes resources ==============")
	
	cur.execute("select p.host_id, primary_name from proxies p, hosts h where h.host_id=p.host_id and h.htype != 'e'")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("\nHost #%s %s:" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('grep -c processor /proc/cpuinfo', stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			cpus = request.perform()['stdout'].rstrip()
			if int(cpus) < 4:
				logging.info("CPUs:\t%s Cores\t\t[  FAILED  ]\t Minimum requirement for UI 4 Cores and 8GB RAM, Please see the requirements for CPUs/RAM at http://download.automation.odin.com/oa/7.0/oapremium/portal/en/hardware_requirements/60434.htm" % cpus)
			else:
				logging.info("CPUs:\t%s Cores\t\t[  OK  ]" % cpus)
			request.command('grep MemTotal /proc/meminfo | grep -o [0-9]*', stdout='stdout', stderr='stderr', valid_exit_codes=[0])
			mem = request.perform()['stdout']
			mem = int(mem)/1000000
			if mem < 8:
				logging.info("RAM:\t%s GB\t\t[  FAILED  ]\t Minimum requirement for UI 4 Cores and 8GB RAM, Please see the requirements for CPUs/RAM at http://download.automation.odin.com/oa/7.0/oapremium/portal/en/hardware_requirements/60434.htm" % str(mem))
			else:
				logging.info("RAM:\t%s GB\t\t[  OK  ]" % str(mem))
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def mem_winnodes():

	logging.info('\n\t============== Checking free memory on WIN nodes (at risk if less 500MB )==============\n')

	cur.execute("select count(1) from hosts where pleskd_id>0 and htype = 'w'")
	logging.info("Number of Win Nodes: %s" % cur.fetchone()[0])
	cur.execute("select host_id, primary_name from hosts where pleskd_id>0 and htype = 'w'")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("Host #%s %s" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('systeminfo |find "Physical Memory"', stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			logging.info(request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def uiprox_misconf():

	logging.info("\n\t============== Checking UI proxies misconfigs in oss DB ==============\n")
	
	cur.execute("select brand_id,proxy_id from brand_proxy_params")
	for row in cur.fetchall():
		brand_id = row[0]
		proxy_id = row[1]
		
		cur.execute("select 1 from proxies where proxy_id = "+str(proxy_id))
		if cur.fetchone() is None:
			logging.info("Checking Brand #%s:\tproxy #%s\t[  FAILED  ]" % (str(brand_id),str(proxy_id)))
		else:
			logging.info("Checking Brand #%s:\tproxy #%s\t[  OK  ]" % (str(brand_id),str(proxy_id)))

def rsync():

	logging.info("\n\t============== Checking rsync on NS nodes ==============\n")

	cur.execute("select s.host_id,primary_name from services s, hosts h where s.name = 'bind9' and h.host_id = s.host_id")
	for row in cur.fetchall():
		host_id = row[0]
		host_name = row[1]
		logging.info("Host #%s %s:" % (str(host_id),host_name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command("rpm -q rsync", stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			logging.info(request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def zones():

	logging.info("\n\t============== Checking bad zones on NS nodes ==============\n")
	
	cur.execute("select s.host_id,primary_name from services s, hosts h where s.name = 'bind9' and h.host_id = s.host_id")
	for row in cur.fetchall():
		host_id = row[0]
		host_name = row[1]
		logging.info("Host #%s %s:" % (str(host_id),host_name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('/usr/sbin/named-checkconf -z -t /var/named/chroot/ /etc/named.conf | grep -i bad || echo "Bad zones not found"', stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
		try:
			logging.info(request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def mess_bodies():

	logging.info("\t============== Checking empty message_bodies ==============\n")
	
	cur.execute("select length(message_body) from message_bodies")
	for row in cur.fetchall():
		if row[0] is None:
			logging.info("Result:\t[  FAILED  ]\tempty message_bodies")
		else:
			logging.info("Result:\t[  OK  ]")
			continue
	
def yum_repos():

	logging.info("\n\t============== Checking YUM repos on all nodes ==============\n")

	cur.execute("select count(1) from hosts where pleskd_id>0 and htype not in ('w','e')")
	total = cur.fetchone()[0]
	num = 0
	logging.info("Number of Nodes: %s" % total)
	cur.execute("select host_id, primary_name from hosts where pleskd_id>0 and htype not in ('w','e')")
	for row in cur.fetchall():
		num+=1
		host_id = row[0]
		name = row[1]
		logging.info("Host #%s %s (%s of %s):" % (str(host_id),name,num,total))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('yum clean all && yum repolist', stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
		try:
			out = request.perform()
			logging.info("%sERRORs:\n%s" % (out['stdout'],out['stderr']))
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on service node\n %s\n" % str(e))
			continue

def java_ver():

	logging.info("\n\t======= Checking Java on UI nodes. Note: Java ver. must be 1.7.0 & libgcj and openjdk should NOT(!) be installed =======\n")

	cur.execute("select p.host_id, primary_name from proxies p, hosts h where h.host_id=p.host_id and h.htype != 'e'")
 
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
                logging.info("           ")
		logging.info("Host #%s %s:" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command("java -version", stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			logging.info(request.perform()['stderr'])
		except Exception, e:
	        	logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
		logging.info("           ")
                request = uHCL.Request(host_id, user='root', group='root')
                request.command('rpm -qa | grep libgcj || echo "No libgcj package found"', stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
                try:
                        logging.info(request.perform()['stdout'])
                except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
                request = uHCL.Request(host_id, user='root', group='root')
                request.command('rpm -qa | grep openjdk || echo "No openjdk packages found"', stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
                try:
                        logging.info(request.perform()['stdout'])
                except Exception, e:
                        logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def num_resources():

	logging.info("\n\t============== Checking number of accounts/users/subs/oa-db size ==============\n")
	
	cur.execute("select count(1) from accounts")
	logging.info("Number of Accounts: %s (If Num > 20K: add additional 1H for every 20K)" % cur.fetchone()[0])
	cur.execute("select count(1) from users")
	logging.info("Number of Users: %s" % cur.fetchone()[0])
	cur.execute("select count(1) from subscriptions")
	logging.info("Number of Subscriptions: %s (If Num > 20K: add additional 1H for every 20K)" % cur.fetchone()[0])
	cur.execute("SELECT rt.restype_name, count(sr.rt_id) as subs, sum(sr.curr_usage) as usage FROM subs_resources sr, resource_types rt WHERE rt.rt_id = sr.rt_id and sr.rt_id not in (select rt_id from resource_types where class_id in (select class_id from resource_classes where name in ('DNS Management','disc_space','traffic','ips','shared_hosting_php','apache_ssl_support','apache_name_based','apache_lve_cpu_usage','proftpd.res.ds','proftpd.res.name_based','rc.saas.resource','rc. saas.resource.mhz','rc.saas.resource.unit','rc .saas.resource.kbps','rc.saas.resource.mbh','rc.saas.resource.mhzh','rc.saas.resource.unith'))) GROUP by rt.restype_name having sum(sr.curr_usage) > 0 ORDER by 2 desc")
	logging.info("Resources Number:\n--------+---------+---------------------------\n subs	| usage	| restype_name\n--------+---------+---------------------------")
	for row in cur.fetchall():
		logging.info(" %s	| %s	| %s " % (row[1], row[2], row[0]))
	cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
	logging.info("\nOA DB size: %s (If size > 15GB: add additional 1H for every 10GB)\n\nShow most fragmented tables and last vacuum time:" % cur.fetchone()[0])
	cur.execute("select a.relname,n_dead_tup, to_char(last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, pg_size_pretty(pg_table_size(b.oid)) as size from pg_stat_all_tables a, pg_class b where a.relname = b.relname order by n_dead_tup desc limit 10")
	logging.info("------------------------+---------------+-------------------------------+-------------------------------+---------\n relname             | n_dead_tup    | last_vacuum\t\t\t| last_autovacuum\t| size\n------------------------+---------------+-------------------------------+-------------------------------+---------")
	for row in cur.fetchall():
		tab1 = tab2 = tab3 = ''
		if      len(str(row[0])) < 7: tab1 = '\t\t'
		elif    len(str(row[0])) < 15: tab1 = '\t'
		if		len(str(row[2])) < 10: tab2 = '\t\t'
		if		len(str(row[3])) < 10: tab3 = '\t\t'
		logging.info(" %s%s    | %s            | %s%s  | %s%s  | %s " % (row[0],tab1, row[1], row[2],tab2, row[3],tab3, row[4]))
		
def ba_res():

	logging.info('\n\t************************************ Checking BA resources ************************************\n')
	
	if not os.path.isfile("poaupdater.tgz"):
		os.system("tar -zcf poaupdater.tgz poaupdater")
	dir_path = os.path.dirname(os.path.realpath(__file__))
	lpath = dir_path+'/poaupdater.tgz'
	bmpath = dir_path+'/bmcheck.py'
	cur.execute("select host_id from hosts where host_id in (select host_id from components where pkg_id in (select pkg_id from packages where name='PBAApplication'))")
	try:
		ba_host_id = cur.fetchone()[0]
		request = uHCL.Request(ba_host_id, user='root', group='root')
		request.transfer('1', bmpath, '/usr/local/bm/tmp/')
		request.transfer('1', lpath, '/usr/local/bm/tmp/')
		request.extract('/usr/local/bm/tmp/poaupdater.tgz', '/usr/local/bm/tmp/')
		request.command('python /usr/local/bm/tmp/bmcheck.py', stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
		try:
			logging.info(request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
	except Exception, e:
		logging.info("BA not deployed")
	
def pg_perf():
	
	logging.info('\n\t=============================== Checking OA-DB Performance ===============================\n')
	
	proc = subprocess.Popen("grep Servername /usr/local/pem/etc/odbc.ini",stdout=subprocess.PIPE,shell=True)
	(dbserv, err) = proc.communicate()
	dbserv = dbserv.rstrip()
	m = re.search('Servername      = (.*)', dbserv)
	if m:
		host = m.group(1)
		ip_host = socket.gethostbyname(host)
		local = os.system("ip a | grep -q %s" % ip_host)
		if local == 0: dbis = "Local"
		else: dbis = "Remote"
		logging.info("OA DB %s (%s)" % (dbserv,dbis))
	else:
		print "pattern not found"
	
	errmsg = None
	pb = uPgBench.PgBench(uSysDB.connect())
	for testcase in ("sequentialSelect", "sequentialCommit"):
		logging.info("Running "+testcase+" test...")
		status, msg = pb.test(testcase)
		if not status:
			errmsg = msg
		logging.info(msg)

	if errmsg is not None:
		logging.warn("Required PostgreSQL performance is not achieved: %s" % errmsg)
	
	logging.info("\nChecking Tables > 100MB without analyze > 1 day:\n")
	old100MBTablesSQL = """
	SELECT
		ts.schemaname AS schema,
		ts.relname AS tablename,
		pg_size_pretty(pg_table_size(ts.relid)) AS tablesize,
		'' || date_trunc('second', now()) - date_trunc('second', GREATEST(ts.last_autoanalyze, ts.last_analyze)) AS last_analyze
	FROM
		pg_stat_user_tables AS ts
	WHERE
		pg_table_size(ts.relid) > 100*1024*1024
		AND
		date_trunc('second', now()) - date_trunc('second', GREATEST(ts.last_autoanalyze, ts.last_analyze)) > interval '1 day'
	ORDER BY
		pg_table_size(ts.relid) DESC
	"""

	cur.execute(old100MBTablesSQL)
	oldStat100MBTables = cur.fetchall()
	if oldStat100MBTables:
		for t in oldStat100MBTables:
			tbl = "\"%s\".\"%s\"" % (t[0], t[1])
			logging.info("Table %s has size %s and was analyzed %s ago."%(tbl, t[2], t[3]))
			logging.info("Perform: ANALYZE %s;" % tbl)
	
	logging.info("\nChecking Tables Bloat > 100MB:\n")
	oldBloated100MBTablesSQL = """
	SELECT Res1.schema,
		   Res1.tablename,
		   pg_size_pretty(Res1.tt_size) table_size,
		   pg_size_pretty(Res1.bloat_size::bigint) bloat_size,
		   CASE Res1.tt_size
			   WHEN 0 THEN '0'
			   ELSE round(Res1.bloat_size::numeric/Res1.tt_size*100, 2)||'%'
		   END bloat_proc,
		   'VACUUM FULL ANALYZE VERBOSE ' || Res1.schema || '."' || Res1.tablename || '";' command_to_run
	FROM
	  (SELECT res0.schema,
			  res0.tablename,
			  pg_table_size(res0.toid) tt_size,
			  pg_total_relation_size(res0.toid) to_size,
			  GREATEST((res0.heappages + res0.toastpages - (ceil(res0.reltuples/ ((res0.bs-res0.page_hdr) * res0.fillfactor/((4 + res0.tpl_hdr_size + res0.tpl_data_size + (2 * res0.ma)
				 - CASE
					 WHEN res0.tpl_hdr_size%res0.ma = 0 THEN res0.ma
					 ELSE res0.tpl_hdr_size%res0.ma
				   END
					   - CASE
						   WHEN ceil(res0.tpl_data_size)::int%res0.ma = 0 THEN res0.ma
						   ELSE ceil(res0.tpl_data_size)::int%res0.ma
						 END)*100))) + ceil(res0.toasttuples/4))) * res0.bs, 0) AS bloat_size,
			  res0.reltuples tbltuples
	   FROM
		 (SELECT tbl.oid toid,
				 ns.nspname AS SCHEMA,
				 tbl.relname AS tablename,
				 tbl.reltuples,
				 tbl.relpages AS heappages,
				 coalesce(substring(array_to_string(tbl.reloptions, ' ')
									FROM '%fillfactor=#"__#"%'
									FOR '#')::smallint, 100) AS fillfactor,
				 coalesce(toast.relpages, 0) AS toastpages,
				 coalesce(toast.reltuples, 0) AS toasttuples,
				 current_setting('block_size')::numeric AS bs,
				 24 AS page_hdr,
				 CASE
					 WHEN version()~'mingw32'
						  OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8
					 ELSE 4
				 END AS ma,
				 bool_or(att.atttypid = 'pg_catalog.name'::regtype) AS is_na,
				 23 + CASE
						  WHEN MAX(coalesce(s.null_frac,0)) > 0 THEN (7 + count(*)) / 8
						  ELSE 0::int
					  END + CASE
								WHEN tbl.relhasoids THEN 4
								ELSE 0
							END AS tpl_hdr_size,
							sum((1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS tpl_data_size
		  FROM pg_class tbl
		  JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
		  JOIN pg_attribute AS att ON att.attrelid = tbl.oid
		  LEFT JOIN pg_class AS TOAST ON toast.oid = tbl.reltoastrelid
		  LEFT JOIN pg_stats AS s ON s.schemaname=ns.nspname
		  AND s.tablename = tbl.relname
		  AND s.inherited=FALSE
		  AND s.attname=att.attname
		  WHERE tbl.relkind = 'r'
			AND ns.nspname NOT IN ('pg_catalog',
								   'information_schema')
			AND att.attnum > 0
			AND NOT att.attisdropped
		  GROUP BY tbl.oid,
				   ns.nspname,
				   tbl.relname,
				   tbl.reltuples,
				   tbl.relpages,
				   fillfactor,
				   toastpages,
				   toasttuples,
				   tbl.relhasoids) Res0) Res1
	WHERE Res1.tt_size > 100*1024*1024
	  AND Res1.bloat_size/Res1.tt_size > 0.5
	ORDER BY Res1.bloat_size DESC
	"""

	cur.execute(oldBloated100MBTablesSQL)
	oldBloated100MBTables = cur.fetchall()
	if oldBloated100MBTables:
		for t in oldBloated100MBTables:
			tbl = "\"%s\".\"%s\"" % (t[0], t[1])
			logging.info("Table %s with size %s has bloat size %s(%s)."%(tbl, t[2], t[3], t[4]))
			logging.info("Perform: VACUUM FULL ANALYZE VERBOSE %s;" % tbl)

def yum_dryrun():

	logging.info('\n\t============================== YUM php-mbstring install dry-run  ==============================\n')
	
	cur.execute("select host_id,primary_name from hosts where host_id in (select host_id from components where pkg_id in (select pkg_id from packages where name in ('PBAApplication','PBAOnlineStore')))")
	for row in cur.fetchall():
		try:
			host_id = row[0]
			hname = row[1]
			logging.info('\nChecking %s node:\n' % hname)
			request = uHCL.Request(host_id, user='root', group='root')
			request.command('yum install php-mbstring', stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
			try:
				out = request.perform()
				logging.info("%sERRORs:\n%s" % (out['stdout'],out['stderr']))
			except Exception, e:
				logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
		except Exception, e:
			logging.info("BA not deployed")

def main():

	parser = optparse.OptionParser()
	parser.add_option("-s", "--skip", metavar="skip", help="phase to skip: diskspace,uires,uiprox,memwin,rsync,yum,java,numres,messg,ba,zones,pgperf,dry")
	parser.add_option("-o", "--only", metavar="only", help="phase to run only: diskspace,uires,uiprox,memwin,rsync,yum,java,numres,messg,ba,zones,pgperf,dry")
	parser.add_option("-l", "--log", metavar="log", help="path to log file, default: current dir")
	opts, args = parser.parse_args()
	skip = opts.skip or ''
	only = opts.only or ''

	filename = time.strftime("/ext_precheck_7.0-%Y-%m-%d-%H%M.txt", time.localtime())
	logfile = opts.log or os.path.abspath(os.path.dirname(__file__)) + filename

	logging.basicConfig(
		level=logging.DEBUG,
		format='%(message)s',
		datefmt='%m-%d %H:%M',
		filename=logfile,
		filemode='w')

	console = logging.StreamHandler()
	console.setLevel(logging.INFO)
	logging.getLogger('').addHandler(console)
	
	disp = {
		'diskspace': diskspace,
		'uires': ui_resources,
		'uiprox': uiprox_misconf,
		'memwin': mem_winnodes,
		'rsync': rsync,
		'yum': yum_repos,
		'java': java_ver,
		'numres': num_resources,
		'messg': mess_bodies,
		'ba': ba_res,
		'zones': zones,
		'pgperf': pg_perf,
		'dry': yum_dryrun
	}
	
	logging.info("\nOA MN Server Name:\t%s\n" % socket.gethostname())
	
	if only != '':
		disp[only]()
	elif skip != '':
		for f in disp.iterkeys():
			if f in skip: continue
			else: disp[f]()
	else:
		for f in disp.iterkeys():
			disp[f]()

	logging.info("\nlog saved to: %s\n" % logfile)

cur = uSysDB.connect().cursor()
main()