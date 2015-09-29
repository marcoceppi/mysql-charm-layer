import os
import sys

from subprocess import check_output

from charmhelpers.fetch import (
    apt_install,
    apt_update,
    add_source,
)

from charmhelpers.core.templating import render
from charmhelpers.contrib.database.mysql import MySQLHelper


def install_mysql(package='mysql-server', sources=None, keys=None):
    if not sources:
        sources = []

    if not keys:
        keys = []

    from subprocess import (
        Popen,
        PIPE,
    )

    for source in sources:
        add_source(source)

    if sources:
        apt_update()

    with open('/var/lib/mysql/mysql.passwd', 'r') as rpw:
        root_pass = rpw.read()

    dconf = Popen(['debconf-set-selections'], stdin=PIPE)
    dconf.stdin.write("%s %s/root_password password %s\n" % (package, package,
                                                             root_pass))
    dconf.stdin.write("%s %s/root_password_again password %s\n" % (package,
                                                                   package,
                                                                   root_pass))
    dconf.communicate()
    dconf.wait()

    apt_install(package)


def build_mycnf(cfg):
    i_am_a_slave = os.path.isfile('/var/lib/juju/i.am.a.slave')
    # REFACTOR add to charm helpers
    unit_id = os.environ['JUJU_UNIT_NAME'].split('/')[1]

    if i_am_a_slave and cfg.get('tuning-level') != 'fast':
        # On slaves, this gets overwritten
        render(
            source='mysql/binlog.cnf',
            target='/etc/mysql/conf.d/binlog.cnf',
            context={
                'unit_id': unit_id,
                'format': cfg.get('binlog-format', 'MIXED')
            },
        )

    render(source='mysql/my.cnf', target='/etc/mysql/my.cnf',
           context=cfg)


def human_to_bytes(human):
    if human.isdigit():
        return human
    factors = {'k': 1024, 'm': 1048576, 'g': 1073741824, 't': 1099511627776}
    modifier = human[-1]
    if modifier.lower() in factors:
        return int(human[:-1]) * factors[modifier.lower()]

    raise ValueError("Can only convert K, M, G, and T")


def dataset_size(size, page):
    if not size.endswith('%'):
        return human_to_bytes(size)

    total_mem = human_to_bytes(get_memtotal())
    sys_mem_limit = mem_limit()
    if is_32bits() and total_mem > sys_mem_limit:
        total_ram = sys_mem_limit

    factor = int(size[:-1]) * 0.01
    pctram = sys_mem_limit * factor
    return int(pctram - (pctram % page))


def is_32bits():
    try:
        IS_32BIT_SYSTEM = sys.maxsize < 2**32.
    except OverflowError:
        IS_32BIT_SYSTEM = True

    return IS_32BIT_SYSTEM


def mem_limit():
    import platform

    SYS_MEM_LIMIT = human_to_bytes(get_memtotal())

    if platform.machine() in ['armv7l']:
        SYS_MEM_LIMIT = human_to_bytes('2700M')  # experimentally determined
    elif is_32bits():
        SYS_MEM_LIMIT = human_to_bytes('4G')

    return SYS_MEM_LIMIT


def get_memtotal():
    with open('/proc/meminfo') as meminfo_file:
        for line in meminfo_file:
            (key, mem) = line.split(':', 2)
            if key == 'MemTotal':
                (mtot, modifier) = mem.strip().split(' ')
                return '%s%s' % (mtot, modifier[0].upper())


def get_db_helper():
    return MySQLHelper(rpasswdf_template='/var/lib/mysql/mysql.passwd',
                       upasswdf_template='/var/lib/mysql/mysql-{}.passwd',
                       delete_ondisk_passwd_file=False)


# REFACTOR factory/cache
def get_db_cursor():
    import MySQLdb
    # Connect to mysql
    db_helper = get_db_helper()
    passwd = db_helper.get_mysql_root_password()
    connection = MySQLdb.connect(user="root", host="localhost", passwd=passwd)
    return connection.cursor()


def create_database(name):
    # REFACTOR UTF-8
    # Clean databasename
    cursor = get_db_cursor()
    cursor.execute("show databases like '%s'" % name)
    if cursor.fetchall():
        return name
    cursor.execute("create database `%s` character set utf8" % name)
    cursor.close()
    return name


def create_user():
    # REFACTOR pwgen python module? maybe? yeah?
    (user, password) = check_output(['pwgen', '-N 2', '15']).split('\n')[:-1]
    cursor = get_db_cursor()
    grant_sql = "grant replication client on *.* to `%s` identified by '%s'"
    cursor.execute(grant_sql % (user, password))
    cursor.close()
    return (user, password)


def grant_database(database, user, password):
    cursor = get_db_cursor()
    cursor.execute(
        "grant all on `%s`.* to `%s` identified by '%s'" % (database,
                                                            user, password))
    cursor.close()



#
#relation_id = os.environ.get('JUJU_RELATION_ID')
#change_unit = os.environ.get('JUJU_REMOTE_UNIT')
#
## We'll name the database the same as the service.
#database_name_file = '.%s_database_name' % (relation_id)
## change_unit will be None on broken hooks
#database_name = ''
#if change_unit:
#    database_name, _ = change_unit.split("/")
#    with open(database_name_file, 'w') as dbnf:
#        dbnf.write("%s\n" % database_name)
#        dbnf.flush()
#elif os.path.exists(database_name_file):
#    with open(database_name_file, 'r') as dbname:
#        database_name = dbname.readline().strip()
#else:
#    print 'No established database and no REMOTE_UNIT.'
## A user per service unit so we can deny access quickly
#lastrun_path = '/var/lib/juju/%s.%s.lastrun' % (database_name, user)
#slave_configured_path = '/var/lib/juju.slave.configured.for.%s' % database_name
#slave_configured = os.path.exists(slave_configured_path)
#slave = os.path.exists('/var/lib/juju/i.am.a.slave')
#broken_path = '/var/lib/juju/%s.mysql.broken' % database_name
#broken = os.path.exists(broken_path)
#
#
#
#
#def migrate_to_mount(new_path):
#    """Invoked when new mountpoint appears. This function safely migrates
#    MySQL data from local disk to persistent storage (only if needed)
#    """
#    old_path = '/var/lib/mysql'
#    if os.path.islink(old_path):
#        hookenv.log('{} is already a symlink, skipping migration'.format(
#            old_path))
#        return True
#    # Ensure our new mountpoint is empty. Otherwise error and allow
#    # users to investigate and migrate manually
#    files = os.listdir(new_path)
#    try:
#        files.remove('lost+found')
#    except ValueError:
#        pass
#    if files:
#        raise RuntimeError('Persistent storage contains old data. '
#                           'Please investigate and migrate data manually '
#                           'to: {}'.format(new_path))
#    os.chmod(new_path, 0o700)
#    if os.path.isdir('/etc/apparmor.d/local'):
#        render('apparmor.j2', '/etc/apparmor.d/local/usr.sbin.mysqld',
#               context={'path': os.path.join(new_path, '')})
#        host.service_reload('apparmor')
#    host.service_stop('mysql')
#    host.rsync(os.path.join(old_path, ''),  # Ensure we have trailing slashes
#               os.path.join(new_path, ''),
#               options=['--archive'])
#    shutil.rmtree(old_path)
#    os.symlink(new_path, old_path)
#    host.service_start('mysql')
