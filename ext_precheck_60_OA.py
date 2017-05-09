#!/usr/bin/python
import re, atexit, sys, os, shutil, codecs, socket, deployment, install_routines, optparse
from poaupdater import uConfig, uAction, uActionContext, uFSReader, uDLModel, uLogging, uSysDB, uPEM, uPrecheck, uUtil, openapi, uPackaging, PEMVersion, uOSCommon, uHCL, uPBA, uBuild, uURLChecker

class bcolors:
    LTBLUE = '\x1b[1;36;40m'
    BLUE = '\x1b[1;34;40m'
    GREEN = '\x1b[1;32;40m'
    WARNING = '\033[93m'
    FAIL = '\x1b[1;31;40m'
    END = '\x1b[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class NotEnoughFreeDiskSpace(Exception):
    def __init__(self, node_name = None, free_space = None, quota_in_gb = None, errorMessage = None):
        if errorMessage:
            self.reason = errorMessage
            Exception.__init__(self, "Precheck error: " + errorMessage)
            return

        self.intro = "\tAvailable disk space is %iGb, required free space is %iGb" % (free_space, quota_in_gb)
        self.fin1 = "\t[  "+bcolors.GREEN+"OK"+bcolors.END+"  ]"
        self.fin2 = "\tnot enough free disk space."
        self.reason = self.intro + self.fin2
        # Using directly Exception.__init__ call for python 2.4 compatibility
        Exception.__init__(self, "Precheck error: " + self.intro + self.fin2)

def getHost(host_id):
    """
    :param host_id:
    :return: uUtil.PEMHost
    """
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "SELECT h.host_id, h.primary_name, h.htype, p.opsys, p.osrel, p.arch, default_rootpath, h.pleskd_id, h.note FROM hosts h JOIN platforms p ON (p.platform_id = h.platform_id) WHERE host_id = %s", host_id)

    row = cur.fetchone()
    return uUtil.PEMHost(row[0], row[1], row[2], uBuild.Platform(row[3], row[4], row[5]), row[6], row[7], row[8])

def ping(host_id):
    ip = getHostCommunicationIP(host_id)
    port = 8352
    try:
        if ip:
            uURLChecker.try_connect((ip, port), 2)
    except socket.error, e:
        raise Exception('Connection to host %s failed (%s). Please ensure host is online and pa-agent service is running on it.' % (ip, e))
    execCtl('pleskd_ctl', ['ping', str(host_id)])

def getHostCommunicationIP(host_id):
    # if htype == 'c', this is not real server, this is H2E WebCluster.
    # const THostType H_H2E = 'e';

    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("select p.value from hosts h, components c, packages pkg, v_props p where h.host_id=c.host_id and c.pkg_id=pkg.pkg_id and p.component_id=c.component_id and h.htype<>'e' and p.name='communication.ip' and pkg.name='pleskd' and c.host_id = %s", host_id)
    row = cur.fetchone()
    if row:
        return row[0]
    return None

def _execCtl(ctlname, fun, *args):
    platform, root = getMNInfo()

    if len(args) == 1 and type(args[0]) in (tuple, list):
        parameters = (args)[0]
    else:
        parameters = args

    command = [os.path.join(root, 'bin', ctlname), '-f', os.path.join(root, 'etc', 'pleskd.props')] + list(parameters)
    return fun(command)

def execCtl(ctlname, *args):
    return _execCtl(ctlname, uUtil.execCommand, *args)

def getMNInfo():
    # cache used by multithread slave updater
	global _MN_info
	_MN_info = None
	if _MN_info is None:
		con = uSysDB.connect()
		_MN_info = getHostInfo(con, 1)

	return _MN_info

def getHostInfo(con, host_id):
    cur = con.cursor()
    cur.execute(
        "SELECT p.opsys, p.osrel, p.arch, h.default_rootpath, p.platform_id FROM platforms p JOIN hosts h ON (h.platform_id = p.platform_id) WHERE h.host_id = %s", host_id)
    row = cur.fetchone()

    if not row:
        raise Exception("Database inconsistency - there is host with id %s!" % host_id)

    platform = uBuild.Platform(row[0], row[1], row[2])
    platform.platform_id = row[4]
    rootpath = row[3]
    cur.close()
    return platform, rootpath

def check_free_disk_space(node_id, quota_in_gb):
    """Checking node availability, checking free disk space on any node"""

    node_name = getHost(node_id).name
    uLogging.debug("Making request to node %s for checking free disk space with df utilite.", node_name)
    try:
        uLogging.debug("Checking if node %s is available" % node_name)
        ping(node_id)
    except uUtil.ExecFailed, e:
        uLogging.warn("Failed to ping node %s: %s" % (node_name, e))
        return

    request = uHCL.Request(node_id, user='root', group='root')
    request.command("df / | awk 'END{print $(NF-2)}'", stdout='stdout', stderr='stderr', valid_exit_codes=[0])
    free_space = int(request.perform()['stdout'])/1048576
    fds_exception = NotEnoughFreeDiskSpace(node_name, free_space, quota_in_gb)
    if free_space >= quota_in_gb:
        print fds_exception.intro + fds_exception.fin1
	log.write("\tAvailable disk space is %sGb, required free space is 1Gb\t[  OK  ]\n" % free_space)
    else:
		#raise fds_exception
        uLogging.debug(fds_exception.intro + fds_exception.fin2)
	log.write("\tAvailable disk space is %sGb, required free space is 1Gb\t[  FAILED  ]\n" % free_space)

def diskspace():
	if 'diskspace' in skip: return
	line = '\n ============== Checking free space on all nodes (Free space > 1GB) ==============\n'
	print line
	log.write("%s\n" % line)
	
	free_space = 1 # GB

	cur.execute("select host_id, primary_name from hosts where pleskd_id>0 and htype not in ('w','e')")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		print "%s%s%s:" % (bcolors.LTBLUE,name,bcolors.END),
		log.write("%s:" % name)
		try:
			check_free_disk_space(host_id, free_space)
		except Exception, e:
			print str(e)+"\n"
			continue
			
def ui_resources():
	if 'uires' in skip: return
	line = "\n ============== Checking UI/MN nodes resources ==============\n"
	print line
	log.write(line)
	
	cur.execute("select p.host_id, primary_name from proxies p, hosts h where h.host_id=p.host_id and h.htype != 'e'")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		print "Host #%s %s%s%s:" % (str(host_id),bcolors.LTBLUE,name,bcolors.END)
		log.write("\nHost #%s %s:\n" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('echo -n `grep -c processor /proc/cpuinfo` && grep MemTotal /proc/meminfo', stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			res = request.perform()['stdout']
			matchObj = re.match( r'(.*)MemTotal:(.*?) kB', res, re.M|re.I)
			cpus = matchObj.group(1)
			mem =  matchObj.group(2).replace(" ", "")
			doclink = 'https://download.automation.odin.com/pa/6.0/doc/portal/6.0/oa/60434.htm'
			if int(cpus) < 4:
				print "CPUs:\t" +cpus+ " Cores\t\t[  "+bcolors.FAIL+"FAILED"+bcolors.END+"  ]\t Minimum requirement for UI 4 Cores, Please see the requirements for CPUs/RAM at "+doclink
				log.write("CPUs:\t%s Cores\t\t[  FAILED  ]\t Minimum requirement for UI 4 Cores, Please see the requirements for CPUs/RAM at %s\n" % (cpus,doclink))
			else:
				print "CPUs:\t"+ cpus + " Cores\t\t[  "+bcolors.GREEN+"OK"+bcolors.END+"  ]"
				log.write("CPUs:\t%s Cores\t\t[  OK  ]\n" % cpus)
			mem = int(mem)/1000000
			if mem < 8:
				print "RAM:\t"+str(mem)+" GB" + "\t\t\t[  "+bcolors.FAIL+"FAILED"+bcolors.END+"  ]\t Minimum requirement for UI 4 Cores, Please see the requirements for CPUs/RAM at "+doclink
				log.write("RAM:\t%s GB\t\t\t[  FAILED  ]\t Minimum requirement for UI 4 Cores, Please see the requirements for CPUs/RAM at %s\n" % (str(mem),doclink))
			else:
				print "RAM:\t"+str(mem)+" GB" + "\t\t\t[  "+bcolors.GREEN+"OK"+bcolors.END+"  ]\n"
				log.write("RAM:\t%s GB\t\t\t[  OK  ]\n" % str(mem)) 
		except Exception, e:
			line2 = "%s pa-agent failed...please check poa.log on the node\n%s %s\n" % (bcolors.FAIL,bcolors.END,str(e))
			print line2
			log.write(line2)
			continue
			
def uiprox_misconf():
	if 'uiprox' in skip: return
	line = '\n ============== Checking UI proxies misconfigs in oss DB ==============\n'
	print line
	log.write("%s\n" % line)
	
	cur.execute("select brand_id,proxy_id from brand_proxy_params")
	for row in cur.fetchall():
		brand_id = row[0]
		proxy_id = row[1]
		
		line2 = "Checking Brand #%s: " % str(brand_id)
		print line2,
		log.write(line2)
		cur.execute("select 1 from proxies where proxy_id = "+str(proxy_id))
		result = cur.fetchone()
		if result is None:
			print "proxy #"+str(proxy_id)+" [  "+bcolors.FAIL+"FAILED"+bcolors.END+"  ]"
			log.write("proxy #%s [  FAILED  ]\n" % str(proxy_id))
		else:
			print "proxy #"+str(proxy_id)+" [  "+bcolors.GREEN+"OK"+bcolors.END+"  ]"
			log.write("proxy #%s [  OK  ]\n" % str(proxy_id))
			
def rsync():
	if 'rsync' in skip: return
	line = '\n ============== Checking rsync on NS nodes ==============\n'
	print line
	log.write("%s\n" % line)

	cur.execute("select s.host_id,primary_name from services s, hosts h where s.name = 'bind9' and h.host_id = s.host_id")

	for row in cur.fetchall():
		host_id = row[0]
		host_name = row[1]
		print  "Host #"+str(host_id)+" "+bcolors.LTBLUE+host_name+bcolors.END+": ",
		log.write("Host #%s %s: " % (str(host_id),host_name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command("rpm -q rsync", stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			res = request.perform()['stdout']
			print res,
			log.write(res)
		except Exception, e:
			line2 = "%s pa-agent failed...please check poa.log on the node\n%s %s\n" % (bcolors.FAIL,bcolors.END,str(e))
			print line2
			log.write(line2)
			continue

def yum_repos():
	if 'yum' in skip: return
	line = '\n ============== Checking YUM repos on all nodes ==============\n'
	print line
	log.write("%s\n" % line)

	cur.execute("select host_id, primary_name from hosts where pleskd_id>0 and htype not in ('w','e')")

	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		print  "Host #"+str(host_id)+" "+bcolors.LTBLUE+name+":"+bcolors.END
		log.write("Host #%s %s:\n" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('yum clean all && yum repolist', stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			res = request.perform()['stdout']
			print res,
			log.write("%s\n" % res)
		except Exception, e:
			line = "%s pa-agent failed...please check poa.log on the node\n%s %s\n" % (bcolors.FAIL,bcolors.END,str(e))
			print line
			log.write("%s\n" % line)
			continue

def num_resources():
	if 'numres' in skip: return
	line = '\n ============== Checking number of accounts/users/subs/oa-db size ==============\n'
	print line
	log.write("%s\n" % line)
	
	cur.execute("select count(1) from accounts")
	line = "Number of Accounts: %s (If Num > 20K: add additional 1H for every 20K)" % cur.fetchone()[0]
	print line
	log.write("%s\n" % line)
	cur.execute("select count(1) from users")
	line = "Number of Users: %s" % cur.fetchone()[0]
	print line
	log.write("%s\n" % line)
	cur.execute("select count(1) from subscriptions")
	line = "Number of Subscriptions: %s (If Num > 20K: add additional 1H for every 20K)" % cur.fetchone()[0]
	print line
	log.write("%s\n" % line)
	cur.execute("SELECT rt.restype_name, count(sr.rt_id) as subs, sum(sr.curr_usage) as usage FROM subs_resources sr, resource_types rt WHERE rt.rt_id = sr.rt_id and sr.rt_id not in (select rt_id from resource_types where class_id in (select class_id from resource_classes where name in ('DNS Management','disc_space','traffic','ips','shared_hosting_php','apache_ssl_support','apache_name_based','apache_lve_cpu_usage','proftpd.res.ds','proftpd.res.name_based','rc.saas.resource','rc. saas.resource.mhz','rc.saas.resource.unit','rc .saas.resource.kbps','rc.saas.resource.mbh','rc.saas.resource.mhzh','rc.saas.resource.unith'))) GROUP by rt.restype_name having sum(sr.curr_usage) > 0 ORDER by 2 desc")
	line = "Resources Number:\n--------+---------+---------------------------\n subs	| usage	| restype_name\n--------+---------+---------------------------"
	print line
	log.write("%s\n" % line)
	for row in cur.fetchall():
		line = " %s	| %s	| %s " % (row[1], row[2], row[0])
		print line
		log.write("%s\n" % line)
	cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
	line = "\nOA DB size: %s (If size > 15GB: add additional 1H for every 10GB)\n\nShow most fragmented tables and last vacuum time:" % cur.fetchone()[0]
	print line
	log.write("%s\n" % line)
	cur.execute("select a.relname,n_dead_tup, to_char(last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, pg_size_pretty(pg_table_size(b.oid)) as size from pg_stat_all_tables a, pg_class b where a.relname = b.relname order by n_dead_tup desc limit 10")
	line = "------------------------+---------------+-------------------------------+-------------------------------+---------\n relname             | n_dead_tup    | last_vacuum\t\t\t| last_autovacuum\t| size\n------------------------+---------------+-------------------------------+-------------------------------+---------"
	print line
	log.write("%s\n" % line)
	for row in cur.fetchall():
		tab1 = tab2 = tab3 = ''
		if      len(str(row[0])) < 7: tab1 = '\t\t'
		elif    len(str(row[0])) < 15: tab1 = '\t'
		if		len(str(row[2])) < 10: tab2 = '\t\t'
		if		len(str(row[3])) < 10: tab3 = '\t\t'
		line = " %s%s    | %s            | %s%s  | %s%s  | %s " % (row[0],tab1, row[1], row[2],tab2, row[3],tab3, row[4])
		print line
		log.write("%s\n" % line)
	
def mem_winnodes():
	if 'memwin' in skip: return
	line = '\n ============== Checking free memory on WIN nodes (at risk if less 500MB )==============\n'
	log.write(line)
	print line

	cur.execute("select host_id, primary_name from hosts where pleskd_id>0 and htype in ('w')")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		print  "Host #"+str(host_id)+" "+bcolors.LTBLUE+name+":"+bcolors.END,
		log.write("Host #%s %s" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('systeminfo |find "Available Physical Memory"', stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			res = request.perform()['stdout']
			print res,
			log.write(res)
		except Exception, e:
			line2 = "%s pa-agent failed...please check poa.log on the node\n%s %s\n" % (bcolors.FAIL,bcolors.END,str(e))
			print line2
			log.write(line2)
			continue
	
uLogging.log_to_console = False
con = uSysDB.connect()
cur = con.cursor()

parser = optparse.OptionParser()
parser.add_option("-s", "--skip", metavar="skip", help="phase to skip: diskspace,uires,uiprox,memwin,rsync,yum,numres")
parser.add_option("-l", "--log", metavar="log", help="path to log file, default: current dir")
opts, args = parser.parse_args()
skip = opts.skip or ''
logfile = opts.log or os.path.abspath(os.path.dirname(__file__)) + '/ext_precheck.txt'
log = codecs.open(logfile, encoding='utf-8', mode='w+')

num_resources()
diskspace()
ui_resources()
mem_winnodes()
uiprox_misconf()
rsync()
yum_repos()

print "\nlog saved to: %s\n" % logfile
log.close()
