import subprocess

from charmhelpers.core.hookenv import (
    config,
    status_set,
    open_port,
    close_port,
    log,
    local_unit,
    leader_get,
    unit_private_ip
)

from charmhelpers.core.host import (
    service_restart,

)
from charms.reactive import (
    hook,
    when,
    when_not,
    set_state,
    remove_state,
    main,
)

from charms.layer import mongodb

config = hookenv.config()

'''Support fucntions'''


def mongo_command(host='localhost', command=None):
    if host is None or command is None:
        return False
    else:
        cmd = 'mongo'
        cmd += ' --host {}'.format(host)
        cmd += ' --eval \'printjson({})\''.format(command)
        log("Executing: {}".format(cmd), level=DEBUG)
        return subprocess.call(cmd, shell=True) == 0


def am_I_primary():
    '''
    Mongo stores db.isMaster().ismaster as 'true' or 'false'
    '''
    cmd = 'mongo'
    cmd += ' --host localhost'
    cmd += ' --eval \'printjson(db.isMaster().ismaster)\''
    log("Executing: {}".format(cmd), level=DEBUG)
    retval = subprocess.check_output(cmd, shell=True)
    return retval == 'true'


'''Hooks and reactive states'''


@when('config.changed.version')
def install():
    cfg = config()
    if mongodb.installed():
        status_set('maintenance',
                   'uninstalling mongodb {}'.format(mongodb.version()))
        m = mongodb.mongodb(cfg.previous('version')).uninstall()
        remove_state('mongodb.installed')
        remove_state('mongodb.ready')

    m = mongodb.mongodb(cfg.get('version'))
    status_set('maintenance', 'installing mongodb')
    m.install()
    set_state('mongodb.installed')


@when('mongodb.installed')
@when_not('mongodb.ready')
def configure():
    c = config()
    m = mongodb.mongodb(c.get('version'))
    m.configure(c)
    service_restart('mongodb')

    if c.changed('port') and c.previous('port'):
        close_port(c.previous('port'))

    open_port(c.get('port'))
    set_state('mongodb.ready')


@when('config.changed')
@when_not('config.changed.version')
def check_config():
    remove_state('mongodb.ready')


@hook('leader-elected')
def initiate_replset():
    retval = False

    if not leader_get('replset.init'):
        repl = config['replset']
        unit = local_unit().split('/')[1]
        addr = unit_private_ip()
        port = config['port']
        conf = '{_id: "{}", members: [{_id: {}, host: "{}:{}"}]}'.format(repl,
                                                                         unit,
                                                                         addr,
                                                                         port)
        retval = mongo_command('localhost rs.initiate({})'.format(conf))
        leader_set('replset.init')

        set_state('mongodb.replset_initated')

        """
        DO SOME CODE HERE TO MAKE SURE IT WORKED
            check if primary, basically
            if not, sleep, check again
            repeat a few times
        """

    return retval


@hook('peer.relation-joined')
def join_replica_set(peer):
    if am_I_primary():
        commmand = "rs.add({})".format(peer.connection_string())
        mongo_command('localhost', commmand)
        log("Added {} to replica set {}").format(peer.connection_string(),
                                                 config['replset'])
    else:
        log("Not replica set primary, hook finished")
        pass  # We can't do anything if we're not repl primary


@hook('peer.relation-{broken, departed}')
@when('mongodb.replset_initated')
def remove_peer_from_set(peer):
    if not am_I_primary():
        log('Relation broken/departed finished')
        return

    unit_info = "{}:{}".format(hookenv.unit_private_ip(), config('port'))
    peer_info = peer.connection_string()
    # If I'm removing myself:
    if unit_info == peer_info:
        retval = mongo_command('localhost', 'rs.stepDown()')
        log('Stepping down as replset leader')
        return

    mongo_command('localhost',
                  'rs.remove({})'.format(peer.connection_string()))


@hook('update-status')
def update_status():
    if mongodb.installed():
        status_set('active', 'mongodb {}'.format(mongodb.version()))
    else:
        status_set('blocked', 'unable to install mongodb')


if __name__ == '__main__':
    main()  # pragma: no cover
