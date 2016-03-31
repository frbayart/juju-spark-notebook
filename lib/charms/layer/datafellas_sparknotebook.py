import os
import jujuresources

from path import Path
from jujubigdata import utils
from subprocess import call
from charmhelpers.core import unitdata, hookenv


class DataFellasNotebook(object):
    """
    This class manages the Spark Notebook deployment steps.
    :param DistConfig dist_config: The configuration container object needed.
    """
    def __init__(self, dist_config=None):
        self.dist_config = dist_config or utils.DistConfig()
        self.resources = {
            'datafellas-notebook': 'datafellas-notebook-%s' % utils.cpu_arch(),
        }
        self.verify_resources = utils.verify_resources(*self.resources.values())

    def is_installed(self):
        return unitdata.kv().get('datafellas-notebook.prepared')

    def install(self, force=False):
        '''
        Create the directories. This method is to be called only once.
        :param bool force: Force the execution of the installation even if this
        is not the first installation attempt.
        '''
        if not force and self.is_installed():
            return

        cmd = "dpkg -i ".format(self.resources['datafellas-notebook'])
        call(cmd.split())

        self.dist_config.add_dirs()
        self.dist_config.add_packages()

        unitdata.kv().set('datafellas-notebook.prepared', True)
        unitdata.kv().flush(True)


    def start(self):
        cmd = "start spark-notebook"
        call(cmd.split())

    def stop(self):
        cmd = "stop spark-notebook"
        call(cmd.split())

    def open_ports(self):
        for port in self.dist_config.exposed_ports('datafellas-notebook'):
            hookenv.open_port(port)

    def close_ports(self):
        for port in self.dist_config.exposed_ports('datafellas-notebook'):
            hookenv.close_port(port)

    def cleanup(self):
        self.dist_config.remove_dirs()
        unitdata.kv().set('datafellas-notebook.installed', False)
