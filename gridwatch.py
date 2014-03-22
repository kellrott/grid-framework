#!/usr/bin/env python

import mesos
import mesos_pb2

import sys
import time
import os
import socket
import json

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "var", "log", "grid_watch.%s.log" % (socket.gethostname()))

import logging
logging.basicConfig(level=logging.INFO, filename=LOG_FILE)

from subprocess import *


class MyExecutor(mesos.Executor):
    def __init__(self):
        mesos.Executor.__init__(self)

    def init(self, driver, arg):
        logging.debug("Starting grid-watcher")

    def launchTask(self, driver, task):
        logging.debug( "Running task %s" % task.task_id.value )
        update = mesos_pb2.TaskStatus()
        update.task_id.value = task.task_id.value   
        update.state = mesos_pb2.TASK_RUNNING
        
        cpu_count = 1
        for res in task.resources:
            if res.name == 'cpus':
                cpu_count = int(res.scalar.value)
        
        update.data = json.dumps( { 'hostname' : socket.gethostname(), 'cpus' : cpu_count } )
        driver.sendStatusUpdate(update)

    def killTask(self, driver, task_id):
        logging.debug( "Killing task %s" % task_id.value )
        update = mesos_pb2.TaskStatus()
        update.task_id.value = task_id.value   
        update.state = mesos_pb2.TASK_FINISHED
        update.data = json.dumps( { 'hostname' : socket.gethostname(), 'task_id' : task_id.value } )
        driver.sendStatusUpdate(update)
    
    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        if message == "shutdown":
            driver.stop()

    def shutdown(self, driver):
        logging.debug( "shutdown" )
        #cleanup()

    def error(self, driver, code, message):
        print "Error: %s" % message

if __name__ == "__main__":
    logging.info( "Starting Grid Watcher" )
    executor = MyExecutor()
    mesos.MesosExecutorDriver(executor).run()
