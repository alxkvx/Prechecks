#!/usr/bin/python
#v.1.2.005

import re, atexit, sys, os, shutil, codecs, deployment, install_routines, optparse, time, logging, subprocess, socket
from poaupdater import uLogging
# Switch off sending debug logs to stdout.
uLogging.debug = uLogging.log_func(None, uLogging.DEBUG)
from poaupdater import uConfig, uSysDB, uPEM, uPrecheck, uUtil, openapi, uHCL, uBilling

def diskspace():
	if only != '' and only !='diskspace': return
	elif 'diskspace' in skip: return
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
	if only != '' and only !='uires': return
	elif 'uires' in skip: return
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
			request.command("free -g | grep Mem | awk '{print $2}'", stdout='stdout', stderr='stderr', valid_exit_codes=[0])
			mem = int(request.perform()['stdout'])
			if mem < 8:
				logging.info("RAM:\t%s GB\t\t[  FAILED  ]\t Minimum requirement for UI 4 Cores and 8GB RAM, Please see the requirements for CPUs/RAM at http://download.automation.odin.com/oa/7.0/oapremium/portal/en/hardware_requirements/60434.htm" % str(mem))
			else:
				logging.info("RAM:\t%s GB\t\t[  OK  ]" % str(mem))
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def mem_winnodes():
	if only != '' and only !='memwin': return
	elif 'memwin' in skip: return
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
	if only != '' and only !='uiprox': return
	elif 'uiprox' in skip: return
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

def zones():
	if only != '' and only !='zones': return
	elif 'zones' in skip: return
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
	if only != '' and only !='messg': return
	elif 'messg' in skip: return
	logging.info("\t============== Checking empty message_bodies ==============\n")
	
	cur.execute("select length(message_body) from message_bodies")
	for row in cur.fetchall():
		if row[0] is None:
			logging.info("Result:\t[  FAILED  ]\tempty message_bodies")
		else:
			logging.info("Result:\t[  OK  ]")
			continue
	
def yum_repos():
	if only != '' and only !='yum': return
	elif 'yum' in skip: return
	logging.info("\n\t============== Checking YUM repos on all nodes ==============\n")

	cur.execute("select count(1) from hosts where pleskd_id>0 and htype not in ('w','e')")
	logging.info("Number of Nodes: %s" % cur.fetchone()[0])
	cur.execute("select host_id, primary_name from hosts where pleskd_id>0 and htype not in ('w','e')")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("Host #%s %s:" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('yum clean all && yum repolist', stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
		try:
			out = request.perform()
			logging.info("%sERRORs:\n%s" % (out['stdout'],out['stderr']))
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def old_pwd_hashs():
	if only != '' and only !='pwds': return
	elif 'pwds' in skip: return
	logging.info("\n\t============== Checking accounts with Crypt pwd hashes   ==============\n")
	
	cur.execute("select * from local_identities where pwd_type='Crypt' and pwd_hash like '$5$%'")
	fl = cur.fetchone()
	if fl is None:
		logging.info("\t[  OK  ]        No Hashes with Crypt type")
	else:
		logging.info("\t[  FAILED  ]    Found Hashes with Crypt type:\n")
		logging.info(" %s       | %s    | %s  | %s      | %s" % (fl[0], fl[1], fl[2], fl[3], fl[4]))
		for row in cur.fetchall():
			logging.info(" %s       | %s    | %s  | %s      | %s" % (row[0], row[1], row[2], row[3], row[4]))
		logging.info("\nFor users above password should be re-set to PA_Standard_Hash using pem.setMemberPassword as example")

def java_ver():
	if only != '' and only !='java': return
	elif 'java' in skip: return
	logging.info("\n\t======= Checking Java on UI nodes. Note: Java ver. must be 1.7.0 & libgcj and openjdk should NOT(!) be installed =======\n")

	cur.execute("select p.host_id, primary_name from proxies p, hosts h where h.host_id=p.host_id and h.htype != 'e'")
 
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
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
	if only != '' and only !='numres': return
	elif 'numres' in skip: return
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
	if only != '' and only !='ba': return
	elif 'ba' in skip: return
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

def pgdb():
	if only != '' and only !='pgdb': return
	elif 'pgdb' in skip: return
	logging.info('\n\t************************************ Checking PgSQL DB Location ************************************\n')
	
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

def balog():
	if only != '' and only !='balog': return
	elif 'balog' in skip: return
	logging.info('\n\t******************************* Transfer BA prechecker.log to MN *******************************\n')
	
	curdir = os.path.dirname(os.path.realpath(__file__))
	bafile = time.strftime("/ba_prechecker-%Y-%m-%d-%H%M.log", time.localtime())
	sfile = curdir + '/prechecker.log'
	dfile = curdir + bafile
	cur.execute("select host_id from hosts where host_id in (select host_id from components where pkg_id in (select pkg_id from packages where name='PBAApplication'))")
	try:
		ba_host_id = cur.fetchone()[0]
		request = uHCL.Request('1', user='root', group='root')
		request.transfer(str(ba_host_id), '/var/log/pa/prechecker.log', curdir)
		try:
			request.perform()
			os.rename(sfile,dfile)
			logging.info("BA prechecker.log file transfererd to %s" % dfile)
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
	except Exception, e:
		logging.info("BA not deployed")
			
parser = optparse.OptionParser()
parser.add_option("-s", "--skip", metavar="skip", help="phase to skip: diskspace,uires,uiprox,memwin,yum,java,numres,messg,ba,zones,pgdb,balog")
parser.add_option("-l", "--log", metavar="log", help="path to log file, default: current dir")
parser.add_option("-o", "--only", metavar="only", help="phase to run only: diskspace,uires,uiprox,memwin,rsync,yum,java,numres,messg,ba,zones,pgdb,balog")
opts, args = parser.parse_args()
skip = opts.skip or ''
only = opts.only or ''

filename = time.strftime("/ext_precheck_7.1-%Y-%m-%d-%H%M.txt", time.localtime())
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

con = uSysDB.connect()
cur = con.cursor()

pgdb()
num_resources()
diskspace()
ui_resources()
mem_winnodes()
uiprox_misconf()
mess_bodies()
ba_res()
java_ver()
zones()
old_pwd_hashs()
yum_repos()
balog()

logging.info("\nlog saved to: %s\n" % logfile)
