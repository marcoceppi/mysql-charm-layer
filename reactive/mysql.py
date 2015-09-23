
import os
import sys
import uuid

from helpers.mysql import (
    dataset_size,
    human_to_bytes,
    build_mycnf,
    create_user,
    create_database,
    grant_database,
    install_mysql
)

from charmhelpers.core import hookenv
from charmhelpers.core.host import (
    mkdir,
    rsync,
    service_running,
    service_start,
    service_stop,
    service_restart,
    lsb_release,
)

from charmhelpers.core.templating import render

from charmhelpers.fetch import (
    apt_install,
    apt_update,
    add_source,
)

from charms.reactive.decorators import when_file_changed

from charms.reactive import (
    hook,
    when,
    when_not,
    set_state,
    is_state,
    remove_state,
    main,
)


config = hookenv.config()
config['passfile'] = '/var/lib/mysql/mysql.passwd'


# REFACTOR THIS (again)

# Going for the biggest page size to avoid wasted bytes.
# InnoDB page size is 16MB
config['page-size'] = 16777216
config.save()


@hook('install')
def install():
    if is_state('mysql.installed') and not is_state('mysql.upgrade'):
        return

    hookenv.status_set('maintenance', 'installing MySQL Charm dependencies')

    # Add archive source of provided
    add_source(config.get('source'), config.get('key'))
    apt_update()

    import glob
    from subprocess import check_output, CalledProcessError

    # Pre-exec
    if os.path.isdir('exec.d'):
        hookenv.status_set('maintenance', 'running exec.d stuff...')
        for f in glob.glob("exec.d/*/charm-pre-install"):
            if not os.access(f, os.X_OK):
                continue
            try:
                hookenv.status_set('maintenance', 'running %s' % f)
                check_output([f])
            except CalledProcessError as exc:
                ## bail out if anyone fails
                hookenv.log('ERROR', str(exc))
                sys.exit(1)
        apt_update()

    hookenv.status_set('maintenance', 'installing MySQL Charm dependencies')

    apt_install(['debconf-utils', 'python-mysqldb', 'uuid', 'pwgen',
                 'dnsutils'])

    if not os.path.isfile(config.get('passfile')):
        dirname = os.path.dirname(config.get('passfile'))
        if not os.path.isdir(dirname):
            os.mkdir(dirname)
        with open(config.get('passfile'), 'w'):
            pass

    os.chmod(config.get('passfile'), 0o600)
    with open(config.get('passfile'), 'a') as fd:
        fd.seek(0, os.SEEK_END)
        if fd.tell() == 0:
            fd.seek(0)
            fd.write(str(uuid.uuid4()))


@hook('config-changed')
def configure():
    mycnf = config.copy()
    hookenv.status_set('maintenance', 'configuring MySQL charm')

    # Add archive source if provided
    if config.changed('source') or config.changed('key'):
        add_source(config.get('source'), config.get('key'))
        apt_update()

    if config.get('prefer-ipv6'):
        if lsb_release()['DISTRIB_CODENAME'].lower() < "trusty":
            hookenv.status_set('blocked',
                               'IPv6 is not supported in charms for Ubuntu '
                               'versions less than Trusty 14.04')
            sys.exit(0)

    try:
        dataset_bytes = dataset_size(config.get('dataset-size'),
                                     config.get('page-size'))
    except ValueError as e:
        hookenv.status_set('blocked', "invalid dataset-size: %s" % e.message)
        sys.exit(0)

    hookenv.status_set('maintenance', 'installing MySQL Package')
    install_mysql()
    hookenv.status_set('maintenance', 'configuring MySQL service')

    hookenv.log('dataset size in bytes %d' % dataset_bytes)
    qc_size = config.get('query-cache-size', 0)

    qc_size_f = (dataset_bytes * 0.20)

    if qc_size <= 0:
        if config.get('query-cache-type') in ('ON', 'DEMAND', ):
            qc_size = int(qc_size_f - (qc_size_f % config.get['page-size']))
        else:
            qc_size = 0

    mycnf['query_cache_size'] = qc_size

    # 5.5 allows the words, but not 5.1
    # REFACTOR CHECK IF WE NEED THIS STILL
    if config.get('query-cache-type') == 'ON':
        mycnf['query_cache_type'] = 1
    elif config.get('query-cache-type') == 'DEMAND':
        mycnf['query_cache_type'] = 2
    else:
        mycnf['query_cache_type'] = 0

    pref_engines = config.get('preferred-storage-engine').lower().split(',')

    chunk_size = int((dataset_bytes - qc_size) / len(pref_engines))
    mycnf['innodb_flush_log_at_trx_commit'] = 1
    mycnf['sync_binlog'] = 1
    engines = {'innodb': 'InnoDB', 'myisam': 'MyISAM'}

    if 'innodb' in pref_engines:
        mycnf['innodb_buffer_pool_size'] = chunk_size
        if config.get('tuning-level') == 'fast':
            mycnf['innodb_flush_log_at_trx_commit'] = 2
    else:
        mycnf['innodb_buffer_pool_size'] = 0

    mycnf['default_storage_engine'] = engines[pref_engines[0]]

    if 'myisam' in pref_engines:
        mycnf['key_buffer'] = chunk_size
    else:
        # Need a bit for auto lookups always
        mycnf['key_buffer'] = human_to_bytes('8M')

    if config.get('tuning-level') == 'fast':
        mycnf['sync_binlog'] = 0
    # REFACTOR
    if config.get('max-connections') == -1:
        mycnf['max_connections'] = '# max_connections = ?'
    else:
        mycnf['max_connections'] = \
            'max_connections = %s' % config.get('max-connections')

    if config.get('wait-timeout') == -1:
        mycnf['wait_timeout'] = '# wait_timeout = ?'
    else:
        mycnf['wait_timeout'] = \
            'wait_timeout = %s'.format(config.get('wait-timeout'))

    if config.get('prefer-ipv6'):
        mycnf['bind_address'] = '::'
    else:
        mycnf['bind_address'] = '0.0.0.0'

    hookenv.status_set('maintenance', 'building MySQL configuration file')
    build_mycnf(mycnf)

    if ('backup_schedule' in config and 'backup_dir' in config and
            config.get('backup_schedule', False)):
        rsync('templates/mysql/mysql_backup.sh', '/usr/local/bin/',
              options=['--executability'])
        if not os.path.exists(config.get('backup_dir')):
            mkdir(config.get('backup_dir'), perms=0o700)
        render('mysql/mysql_backup.j2',
               '/etc/cron.d/mysql_backup', config)
    else:
        for path in ['/etc/cron.d/mysql_backup',
                     '/usr/local/bin/mysql_backup.sh']:
            if os.path.exists(path):
                os.unlink(path)

    hookenv.status_set('active', 'ready')



@when_file_changed('/etc/mysql/my.cnf', '/etc/mysql/conf.d/binlog.cnf')
def file_change():
    restart()


@hook('start')
def restart():
    if service_running('mysql'):
        service_restart('mysql')
    else:
        service_start('mysql')


@hook('stop')
def stop():
    service_stop('mysql')


@when('db.database.requested')
def db_data(mysql):
    for service in mysql.requested_databases():
        db_name = create_database(service)
        user, password = create_user()
        grant_database(db_name, user, password)
        mysql.provide_database(
            service=service,
            host=hookenv.unit_private_ip(),
            port=3306,
            user=user,
            password=password,
            database=db_name,
            # slave=??
        )

# Store new values in relation settings.
#subprocess.call(
#    ["relation-set",
#     "database=%s" % database_name,
#     "user=%s" % user,
#     "password=%s" % service_password,
#     'host=%s' % hostname,
#     'slave=%s' % slave,])

#if hookenv.relations_of_type('nrpe-external-master'):
#    import nrpe_relations
#    nrpe_relations.nrpe_relation()

if __name__ == '__main__':
    main()
