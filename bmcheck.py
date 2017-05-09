import os, sys, re, collections
from poaupdater import uSysDB, uLogging, uUtil, uPrecheck
from ConfigParser import RawConfigParser, MissingSectionHeaderError
from tempfile import SpooledTemporaryFile

uLogging.log_to_console = False

PBA_ROOT = '/usr/local/bm'

class _ConfigReader(RawConfigParser):
    OPTCRE = re.compile(r'(?P<option>[^=\s][^=]*)'  # Redefined RawConfigParser.OPTCRE
                        r'\s*(?P<vi>[=])\s*'        # to allow '::1 = true' in conf/_amt_service_.res
                        r'(?P<value>.*)$')

    def __init__(self):
        RawConfigParser.__init__(self, dict_type=collections.OrderedDict)
        self.optionxform = str

    def read(self, path):
        try:
            RawConfigParser.read(self, path)
        except MissingSectionHeaderError:  # workaround if ini file contains parameters without section
            with SpooledTemporaryFile('rw') as wf:
                wf.write('[DEFAULT]\n')
                with open(path) as f:
                    wf.write(f.read())
                wf.seek(0)
                self.readfp(wf)

    def set(self, section, option, value):
        if not section in self.sections():
            self.add_section(section)
        RawConfigParser.set(self, section, option, value)

    def update(self, options):
        for opt in options:
            if options[opt] is not None:
                section, option = opt.split('.')
                self.set(section, option, options[opt])

class ConfigReader(_ConfigReader):
    __path = None

    def __init__(self, path):
        _ConfigReader.__init__(self)
        self.__path = path
        self.read(path)

    def save(self):
        with open(self.__path, 'w') as f:
            self.write(f)

class DBConfig:
    def __init__(self, dbhost, name, user, password, type, odbc_driver=None):
        self.database_host = dbhost
        self.database_port = ''
        self.database_name = name
        self.dsn_login = user
        self.dsn_passwd = password
        self.database_type = type
        self.database_odbc_driver = odbc_driver
        self.reinstall = False
		
path_global_conf = os.path.join(PBA_ROOT, 'etc', 'ssm.conf.d', 'global.conf')
if os.path.exists(path_global_conf):
	CONF = ConfigReader(path_global_conf)
	from ConfigParser import NoSectionError
	try:
		DBCONF = DBConfig(CONF.get('environment', 'DB_HOST'), CONF.get('environment', 'DB_NAME'), CONF.get('environment', 'DB_USER'), CONF.get('environment', 'DB_PASSWD'), 'PGSQL')
	except NoSectionError, e:
		uLogging.info("Could not get DB parameters: '%s'. It is OK for Templatestore role." % e)

def plan_len():
	print "\t===== Checking Plan names length ( > 480) =====\n"

	cur.execute("select count(1) from `Language`")
	numlangs = cur.fetchone()[0]

	if numlangs > 1:
		cur.execute("select `PlanID`,name from `Plan`")
		fail = 0
		for row in cur.fetchall():
			planid = row[0]
			longname = row[1]
			matchObj = re.match( r'(en|ru|nl|pt|es|fr|it|jp|de) .*\t.*', longname, re.M|re.I)
			if matchObj:
				matchObj2 = re.match( r'(en|ru|nl|pt|es|fr|it|jp|de) (.*?)\t(en|ru|nl|pt|es|fr|it|jp|de)? ?.*', longname, re.M|re.I)
				if matchObj2:
					name = matchObj2.group(1)
					limit = 476/numlangs
					if len(name) > limit:
						fail = 1
						print "Plan ID: %s %s (len: %s) too long(limit: %s), make it shorter" % (planid,name,len(name),limit)
				else:   print "no plan match2: ID: %s plan: %s" % (planid,longname)
			else:
				print "no match(Custom Language?): ID: %s plan: %s" % (planid,longname)
				fail = 1
		if fail == 0:
			print "Result: [  OK  ]"
	else:
		print "Result: [  OK  ]  # just 1 lang installed"

def orphan_acc():
	print "\n\t===== Checking orphan Accounts =====\n"

	cur.execute("select `ResCatID`, `Vendor_AccountID` from `ResourceCategory` where `ResCatID` in (select distinct `ResCatID` from `BMResourceInCategory`) and `Vendor_AccountID` not in (select `AccountID` from `Account`)")
	if cur.fetchone() is None:
		print "Result: [  OK  ]"
	else:
		print "The list of orphan Accounts:\n\tResCatID\t|\tVendor_AccountID"
		for row in cur.fetchall():
			print "\t%s\t|\t%s" % (row[0],row[1])

def db_size():
	print "\n\t===== Checking DB size =====\n"

	cur.execute("SELECT pg_size_pretty(pg_database_size('pba'))")
	print "BA DB size: %s (If size > 15GB: add additional 1H for every 10GB)" % cur.fetchone()[0]
	print "\nShow most fragmented tables and last vacuum time:"
	cur.execute("select a.relname,n_dead_tup, to_char(last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, pg_size_pretty(pg_table_size(b.oid)) as size from pg_stat_all_tables a, pg_class b where a.relname = b.relname order by n_dead_tup desc limit 10")
	print "------------------------+---------------+-----------------------+-----------------------+---------\n relname\t\t| n_dead_tup\t| last_vacuum\t\t| last_autovacuum\t| size\n------------------------+---------------+-----------------------+-----------------------+---------"
	for row in cur.fetchall():
		tab1 = tab2 = tab3 = ''
		if      len(str(row[0])) < 7: tab1 = '\t\t'
		elif    len(str(row[0])) < 15: tab1 = '\t'
		if      len(str(row[2])) < 10: tab2 = '\t\t'
		if      len(str(row[3])) < 10: tab3 = '\t\t'
		print " %s%s\t| %s\t\t| %s%s\t| %s%s\t| %s " % (row[0],tab1, row[1], row[2],tab2, row[3],tab3, row[4])
		
uSysDB.init(DBCONF)
connection = uSysDB.connect()
cur = connection.cursor()

plan_len()
orphan_acc()
db_size()