#!/usr/bin/env python

import mesos
import mesos_pb2
import os
import sys
import time
import re
import json
import socket
import gridlib
import time
import logging
import logging.handlers

import threading
from daemonize import Daemonize

from optparse import OptionParser
from subprocess import *
from socket import gethostname

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

class GridScheduler(mesos.Scheduler):
    def __init__(self):
        mesos.Scheduler.__init__(self)
        self.lock = threading.RLock()
        self.servers = {}
        self.overloaded = False
        self.grid = gridlib.GridEngine()
        self.executor = self.getExecutorInfo()
        self.jobs = {}
        
    def getHostSlots(self):
        hostSlots = {}
        for h in self.grid.getQueueSlots():
            hostSlots[h['hostname']] = h['slots_used']
        return hostSlots
    
    def adjustSlots(self):
        self.lock.acquire()
        hostSlots = {}
        for j in self.jobs:
            hostSlots[ self.jobs[j]['hostname'] ] = hostSlots.get(self.jobs[j]['hostname'], 0) + self.jobs[j]['cpus']        
        
        for h in self.grid.getQueueSlots():
            if h['hostname'] in hostSlots:
                self.grid.setQueueSlots( "all.q@" + h['hostname'], hostSlots[h['hostname']] )
            else:
                self.grid.setQueueSlots( "all.q@" + h['hostname'], 0 )
        self.lock.release()


    def getExecutorInfo(self):
        execPath = os.path.join(BASE_DIR, "gridwatch.sh")
        driverlog.info("in getExecutorInfo, setting execPath = " + execPath)
        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "gridwatch"
        executor.command.value = execPath
        executor.name = "gridwatch"
        executor.source = "grid-framework"        
        return executor
        
    def getTaskInfo(self, offer, jobid, jobname, accept_cpu, accept_mem):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = "%s:%s" % (offer.hostname,  jobid)
        task.slave_id.value = offer.slave_id.value
        task.name = "%s:%s:%s" % (offer.hostname, jobid, jobname)
        task.executor.MergeFrom(self.executor)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = accept_cpu

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = accept_mem        
        return task
    

    def registered(self, driver, fid, masterInfo):
        driverlog.info("Mesos grid-framwork registered with frameworkID %s" % fid.value)

    def resourceOffers(self, driver, offers):
        self.lock.acquire()
        driverlog.debug("Got %s slot offers" % len(offers))

        jobs_info = self.grid.getJobs()
        exec_hosts = self.grid.getExecHosts()
        host_slots = self.getHostSlots()

        for offer in offers:
            tasks = []
            if offer.hostname in exec_hosts:
                cpu_count = 1
                for res in offer.resources:
                    if res.name == 'cpus':
                        cpu_count = int(res.scalar.value)
                req_cpu = 0
                
                for q in jobs_info:
                    if q['id'] not in self.jobs:
                        if q['state'] == 'pending':
                            if q['slots'] <= cpu_count - req_cpu:
                                task = self.getTaskInfo(offer, q['id'], q['name'], q['slots'], q['slots'] * 1024)
                                self.jobs[q['id']] = { 'hostname': offer.hostname, 'task_id' : task.task_id, 'cpus' : q['slots'] }
                                driverlog.info("Allocating %s with %s CPUs" % (offer.hostname, q['slots']))
                                tasks.append(task)
                                req_cpu += q['slots']
                        elif q['state'] == 'running':
                            if q['slots'] <= cpu_count - req_cpu:
                                if q['id'] not in self.jobs and q['hostname'] == offer.hostname:
                                    #accept this offer to cover cpus used by grid engine, but not allocated
                                    #this happens if grid engine was running prior to grid-framework management
                                    task = self.getTaskInfo(offer, q['id'], q['name'], q['slots'], q['slots'] * 1024)                                            
                                    self.jobs[q['id']] = { 'hostname': offer.hostname, 'task_id' : task.task_id, 'cpus' : q['slots'] }
                                    driverlog.info("Allocating %s with %s CPUs for previously running job" % (offer.hostname, q['slots']))
                                    tasks.append(task)   
                                    req_cpu += q['slots']
            else:
                driverlog.debug("Skipping non SGE offer %s for %d cpus" % (offer.id, cpu_count)) 
            status = driver.launchTasks(offer.id, tasks)
        self.lock.release()
                        
                        
    def statusUpdate(self, driver, status):
        if status.state == mesos_pb2.TASK_RUNNING:
            driverlog.info("Task %s, slave %s is RUNNING" % (status.task_id.value, status.slave_id.value))
            self.adjustSlots()
        if status.state == mesos_pb2.TASK_FINISHED:
            driverlog.info("Task %s, slave %s is FINISHED" % (status.task_id.value, status.slave_id.value))
            self.adjustSlots()
            
    def getFrameworkName(self, driver):
        return "Mesos Torque Framework"



def monitor(sched, driver):
    while True:
        time.sleep(1)
        sched.lock.acquire()        
        jobs_info = sched.grid.getJobs()          
        #terminate tasks that don't match running job allocations
        for j in jobs_info:
            if j['id'] in sched.jobs:
                if j['state'] == 'running':
                    if sched.jobs[j['id']]['hostname'] != j['hostname']:
                        monitorlog.debug("Misaligned task: %s (%s != %s)" % (sched.jobs[j['id']]['task_id'], sched.jobs[j['id']]['hostname'], j['hostname']) )
                        driver.killTask( sched.jobs[j['id']]['task_id'] )
                        del sched.jobs[j['id']]
        
        #delete tasks that are linked to jobs that don't exist
        for j in sched.jobs.keys():
            if not any( ( a['id'] == j for a in jobs_info ) ):
                monitorlog.debug("Removing task: %s" % (sched.jobs[j]['task_id']))
                driver.killTask( sched.jobs[j]['task_id'] )
                del sched.jobs[j]    
        
        sched.lock.release()
        sched.adjustSlots()
    

def main():
    monitorlog.info("starting gridwatch")
    monitorlog.info("Connecting to mesos master %s" % args[0])

    sched = GridScheduler()

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "GridScheduler"

    driver = mesos.MesosSchedulerDriver(sched, framework, args[0])
    threading.Thread(target = monitor, args=[sched, driver]).start()
    driver.run()
    


if __name__ == "__main__":
    pid = "/tmp/grid-framework.pid"
 
    parser = OptionParser(usage = "Usage: %prog mesos_master")
    parser.add_option("-f", action="store_true", help="Don't Daemonize", default=False)
    parser.add_option("-c", action="store_true", help="Log to console", default=False)
    parser.add_option("-v", action="store_true", help="Verbose logging", default=False)
    

    (options,args) = parser.parse_args()
    if len(args) < 1:
        print >> sys.stderr, "At least one parameter required."
        print >> sys.stderr, "Use --help to show usage."
        exit(2)

    LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "var", "log", "grid_framework.log")

    eventlog = logging.getLogger("event_logger")
    eventlog.setLevel(logging.INFO)
    if options.c:
        fh = logging.StreamHandler()
    else:
        fh = logging.FileHandler(LOG_FILE,'w') #create handler
    fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    eventlog.addHandler(fh)

    driverlog = logging.getLogger("driver_logger")
    if options.v:
        driverlog.setLevel(logging.DEBUG)
    else:
        driverlog.setLevel(logging.INFO)       
    driverlog.addHandler(fh)

    monitorlog = logging.getLogger("monitor_logger")
    if options.v:
        monitorlog.setLevel(logging.DEBUG)
    else:
        monitorlog.setLevel(logging.INFO)
    monitorlog.addHandler(fh)
  
    #start by zero'ing out the queue slots, because we haven't allocated
    #any resources with mesos yet
    
    grid = gridlib.GridEngine()
    for q in grid.getQueueSlots():
        grid.setQueueSlots(q['name'], 0)
  
    if (options.f):
        main()
    else:
        daemon = Daemonize(app="grid-framework", pid=pid, action=main) 
        daemon.start()

