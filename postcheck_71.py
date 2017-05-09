#!/usr/bin/python
# v.1.0.005

import atexit, sys, os, shutil, codecs, deployment, install_routines, optparse, time, logging, glob
from poaupdater import uLogging

# Switch off sending debug logs to stdout.
uLogging.debug = uLogging.log_func(None, uLogging.DEBUG)

from poaupdater import uConfig, uSysDB, uPEM, uPrecheck, uUtil, openapi, uHCL, uBilling, uTasks

def diskspace():
	if 'diskspace' in skip: return
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

def uiprox_misconf():
	if 'uiprox' in skip: return
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

def cep_task():
	if 'cep' in skip: return
	logging.info("\n\t============== Scheduling CEP report task ==============\n")
	
	a = openapi.OpenAPI()
	cur.execute("select task_id from tm_tasks where name = 'Report usage data' and method = 'process'")
	task = cur.fetchone()[0]
	try:
		logging.info("rescheduling task id: %i " % task)
		a.pem.restartJob(task_id=task)
	except openapi.OpenAPIError as e:
		if 'task is already running' in e.error_message: pass
		elif 'does not exist' in e.error_message: pass
		else: raise

def find_upd_log():
	for file in glob.glob('/var/log/pa/update*'):
		with open(file, 'r') as searchfile:
			for line in searchfile:
				if 'Odin Automation has been successfully updated to version oa-7.1-3256.' in line:
					return file
		
def parse_upd_log():
	if 'parselog' in skip: return
	logging.info("\n\t============== Parsing Upgrade log file ==============\n")

	file = find_upd_log()
	pre=main=prepkg=post=cleanup = 0
	logging.info("this is upgrade log: %s\nchecking upgrade phases:\n" % file)
	with open(file, 'r') as f:
		fline = f.readline()
		logging.info("first line: %s" % fline)
		for line in f:
			if '[pre]' in line and pre == 0:
				logging.info(line)
				pre = 1
			elif '[main]' in line and main == 0:
				logging.info(line)
				main = 1
			elif '[pre-pkg]' in line and prepkg == 0:
				logging.info(line)
				prepkg = 1
			elif '[post]' in line and post == 0:
				logging.info(line)
				post = 1
			elif '[cleanup]' in line and cleanup == 0:
				logging.info(line)
				cleanup = 1
		last = line
		logging.info("Last line: %s" % last)

def failed_tasks():
	if 'tasks' in skip: return
	logging.info("\n\t============== Checking Task Manager ==============\n")
	
	logging.info("Number of Active Tasks:\t\t\t%s" % uTasks.getNumberOfActiveTasks())
	logging.info("Number of Unprocessed Install Tasks:\t%s" % uTasks.get_num_of_unfinished_installation_tasks())
	logging.info("Number of Failed Tasks:\t\t\t%s" % uTasks.getNumberOfFailedTasks())

	print "Failed Install tasks:\t\t\t",
	cur.execute("SELECT task_id,subscription_id,next_start,name FROM tm_tasks WHERE status = 'f' and name like 'Install %' order by next_start desc")
	row = cur.fetchone()
	if row is not None:
		logging.info("\n--------+----------+---------------------------------------------\nTask_id | Sub_id\t| Next Start\t|\tName\n--------+----------+---------------------------------------------")
		tab = ''
		if len(str(row[1])) < 3: tab = '\t '
		logging.info("%s\t| %s%s | %s | %s" % (row[0],row[1],tab,row[2],row[3]))
		for row in cur.fetchall():
			if len(str(row[1])) < 3: tab = '\t '
			logging.info("%s\t| %s%s | %s | %s" % (row[0],row[1],tab,row[2],row[3]))
	else:
		logging.info("[  OK  ]\tNo failed install tasks.")

	logging.info("Failed tasks for the last day:\t\t")
	cur.execute("SELECT t.task_id,subscription_id,next_start,name FROM tm_tasks t JOIN tm_usual u ON (t.task_id = u.task_id) WHERE t.status in ('f') and next_start > (CURRENT_TIMESTAMP - INTERVAL '1 day') order by next_start desc")
	row = cur.fetchone()
	if row is not None:
		logging.info("\n--------+----------+---------------------------------------------\nTask_id | Sub_id\t| Next Start\t|\tName\n--------+----------+---------------------------------------------")
		tab = ''
		if len(str(row[1])) < 3: tab = '\t '
		logging.info("%s\t| %s%s | %s | %s" % (row[0],row[1],tab,row[2],row[3]))
		for row in cur.fetchall():
			if len(str(row[1])) < 3: tab = '\t '
			logging.info("%s\t| %s%s | %s | %s" % (row[0],row[1],tab,row[2],row[3]))
	else:
		logging.info("[  OK  ]\tNo failed tasks.")
		
def check_ui_services():
	if 'checkui' in skip: return
	logging.info("\n\t============== Checking UI services ==============")
	
	cur.execute("select p.host_id, primary_name from proxies p, hosts h where h.host_id=p.host_id and h.htype != 'e'")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("\nHost #%s %s:" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('service pau status && netstat -ntpl | grep 8080', stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
		try:
			logging.info(request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check agent core.log on the node\n %s\n" % str(e))
			continue

def check_apache_services():
	if 'checkhttpd' in skip: return
	logging.info("\n\t============== Checking Apache services ==============")
	
	cur.execute("SELECT h.host_id, primary_name FROM services s, hosts h WHERE h.host_id=s.host_id and s.name like 'Apache%' or h.host_id in (select host_id from components where pkg_id in (select pkg_id from packages where name in ('PBAApplication','PBAOnlineStore'))) GROUP by 1")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("\nHost #%s %s:" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command("service httpd status && netstat -ntpl | egrep ':80 |:443'", stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
		try:
			logging.info(request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def check_store_php():
	if 'checkstore' in skip: return
	logging.info("\n\t============== Checking Online Store ==============")
	
	cur.execute("select host_id,primary_name from hosts where host_id in (select host_id from components where pkg_id in (select pkg_id from packages where name in ('PBAOnlineStore')))")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("\nOnline Store Host #%s %s:\n\n*** Checking that /usr/share/php/HTMLPurifier included in php.ini/php.d:\n" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command("php -v; echo; php -i | grep 'include_path.*HTMLPurifier'", stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
		try:
			logging.info("Result:\n%s" % request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def check_bind_services():
	if 'checkbind' in skip: return
	logging.info("\n\t============== Checking BIND9 services ==============")
	
	cur.execute("select h.host_id, primary_name from services s, hosts h where h.host_id=s.host_id and name = 'bind9'")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("\nHost #%s %s:" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command("service named status && netstat -ntpl | grep ':53 '", stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
		try:
			logging.info(request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

parser = optparse.OptionParser()
parser.add_option("-s", "--skip", metavar="skip", help="phase to skip: diskspace,uiprox,cep,parselog,tasks,checkui,checkhttpd,checkstore,checkbind")
parser.add_option("-l", "--log", metavar="log", help="path to log file, default: current dir")
opts, args = parser.parse_args()
skip = opts.skip or ''

filename = time.strftime("/postcheck-%Y-%m-%d-%H%M.txt", time.localtime())
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

diskspace()
uiprox_misconf()
cep_task()
parse_upd_log()
failed_tasks()
check_ui_services()
check_apache_services()
check_store_php()
check_bind_services()

logging.info("\nlog saved to: %s\n" % logfile)