# pylint: disable=unused-argument
from charms.reactive import when, when_not
from charms.reactive import set_state, remove_state
from charmhelpers.core import hookenv
from charms.layer.datafellas_notebook import DataFellasNotebook
#
import pwd
import os
from charmhelpers.core.hookenv import status_set
from charmhelpers.core.templating import render

@when('java.installed')
@when_not('datafellas_notebook.installed')
def install_datafellas_notebook():
    dfnb = DataFellasNotebook()
    if dfnb.verify_resources():
        hookenv.status_set('maintenance', 'Installing Spark Notebook')
        dfnb.install()
        set_state('datafellas_notebook.installed')

@when('spark-notebook.installed', 'java.installed')
@when_not('datafellas_notebook.started')
def configure_sparknotebook(spark):
    hookenv.status_set('maintenance', 'Setting up Spark Notebook')
    dfnb = DataFellasNotebook()
    dfnb.start()
    dfnb.open_ports()
    set_state('datafellas_notebook.started')
    hookenv.status_set('active', 'Ready')

@when('spark-notebook.started')
@when_not('spark.ready')
def stop_sparknotebook():
    hookenv.status_set('maintenance', 'Stopping Spark Notebook')
    dfnb = DataFellasNotebook()
    dfnb.close_ports()
    dfnb.stop()
    remove_state('datafellas_notebook.started')

@when_not('spark.joined')
def report_blocked():
    hookenv.status_set('blocked', 'Waiting for relation to Apache Spark')

@when('spark.joined')
@when_not('spark.ready')
def report_waiting(spark):
    hookenv.status_set('waiting', 'Waiting for Apache Spark to become ready')
