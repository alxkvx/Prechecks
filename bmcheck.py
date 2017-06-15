import os, sys, re, collections
from poaupdater import uSysDB, uLogging, uUtil, uPrecheck, uPgBench
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

class MultipleConfigReader(_ConfigReader):
	VAR = re.compile('^\$\((.*)\)$')
	
	def __init__(self, pathes=[]):
		_ConfigReader.__init__(self)
		resolved_ref_options = dict()
		for path in pathes:
			tmp = _ConfigReader()
			tmp.read(path)
			for section in tmp.sections():
				for option in tmp.options(section):
					m = MultipleConfigReader.VAR.match(tmp.get(section, option))
					if m:  # simple resolve values like $(HOST_IP), not recursive, not complrx string
						def resolve_ref_option(section, option, parent_cr, ref_option):
							if parent_cr.has_option(section, ref_option):
								resolved_ref_options['%s.%s' % (section, option)] = parent_cr.get(section, ref_option)
								return True
							return False
		
						for parent_cr in [tmp, self]:
							if resolve_ref_option(section, option, parent_cr, m.group(1)):
								break
		
			self.read(path)
			self.update(resolved_ref_options)

path_dot_global_conf = os.path.join(PBA_ROOT, 'etc/ssm.conf.d/.global.conf')
path_global_conf = os.path.join(PBA_ROOT, 'etc/ssm.conf.d/global.conf')
if os.path.exists(path_dot_global_conf) or os.path.exists(path_global_conf):
	CONF = MultipleConfigReader([path_dot_global_conf, path_global_conf])
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
	
def pg_perf():
	print '\n\t************************************ Checking PgSQL DB Performance ************************************\n'
	
	errmsg = None
	pb = uPgBench.PgBench(uSysDB.connect())
	for testcase in ("sequentialSelect", "sequentialCommit"):
		print "Running "+testcase+" test..."
		status, msg = pb.test(testcase)
		if not status:
			errmsg = msg
		print msg
	
	if errmsg is not None:
		print "Required PostgreSQL performance is not achieved: %s" % errmsg
	
	print "\nChecking Tables > 100MB without analyze > 1 day:\n"
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
			print "Table %s has size %s and was analyzed %s ago."%(tbl, t[2], t[3])
			print "Perform: ANALYZE %s;" % tbl
	
	print "\nChecking Tables Bloat > 100MB:\n"
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
			print "Table %s with size %s has bloat size %s(%s)."%(tbl, t[2], t[3], t[4])
			print "Perform: VACUUM FULL ANALYZE VERBOSE %s;" % tbl
	
uSysDB.init(DBCONF)
connection = uSysDB.connect()
cur = connection.cursor()

plan_len()
orphan_acc()
db_size()
pg_perf()