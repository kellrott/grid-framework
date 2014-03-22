
import subprocess
from xml.dom.minidom import parseString

def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)

def dom_scan(node, query):
    stack = query.split("/")
    if node.localName == stack[0]:
        return dom_scan_iter(node, stack[1:], [stack[0]])

def dom_scan_iter(node, stack, prefix):
    if len(stack):
        for child in node.childNodes:
                if child.nodeType == child.ELEMENT_NODE:
                    if child.localName == stack[0]:
                        for out in dom_scan_iter(child, stack[1:], prefix + [stack[0]]):
                            yield out
                    elif '*' == stack[0]:
                        for out in dom_scan_iter(child, stack[1:], prefix + [child.localName]):
                            yield out
    else:
        if node.nodeType == node.ELEMENT_NODE:
            yield node, prefix, dict(node.attributes.items()), getText( node.childNodes )
        elif node.nodeType == node.TEXT_NODE:
            yield node, prefix, None, getText( node.childNodes )


class GridEngine:

	def getJobs(self):
		p = subprocess.Popen(['qstat', '-u', '*', '-xml'], stdout=subprocess.PIPE)
		(stdoutdata, stderrdata) = p.communicate()
		dom=parseString(stdoutdata)
		root_node = dom.childNodes[0]
		out = []

		#scan running jobs
		for node, stack, attr, text in dom_scan(root_node, 'job_info/queue_info/job_list'):
			e = {'state' : attr['state']}
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "job_list/slots"):
				e['slots'] = int(s_text)
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "job_list/JB_job_number"):
				e['id'] = s_text
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "job_list/JB_name"):
				e['name'] = s_text
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "job_list/queue_name"):
				e['queue_name'] = s_text
				e['hostname'] = s_text.split("@")[1]
			out.append(e)
		
		#scan pending jobs
		for node, stack, attr, text in dom_scan(root_node, 'job_info/job_info/job_list'):
			e = {'state' : attr['state']}
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "job_list/JB_name"):
				e['name'] = s_text
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "job_list/slots"):
				e['slots'] = int(s_text)
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "job_list/JB_job_number"):
				e['id'] = s_text
			out.append(e)
		return out
	
	def getQueueSlots(self):
		p = subprocess.Popen(['qstat', '-u', '*', '-f', '-xml'], stdout=subprocess.PIPE)
		(stdoutdata, stderrdata) = p.communicate()
		dom=parseString(stdoutdata)
		root_node = dom.childNodes[0]
		out = []
		for node, stack, attr, text in dom_scan(root_node, 'job_info/queue_info/Queue-List'):
			e = {}
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "Queue-List/name"):
				e['name'] = s_text	
				e['hostname'] = s_text.split("@")[1]		
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "Queue-List/slots_total"):
				e['slots_total'] = int(s_text)
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "Queue-List/slots_used"):
				e['slots_used'] = int(s_text)
			for s_node, s_stack, s_attr, s_text in dom_scan(node, "Queue-List/slots_total"):
				e['slots_total'] = int(s_text)
			out.append(e)
		return out

	def getExecHosts(self):
		p = subprocess.Popen(['qconf', '-sel'], stdout=subprocess.PIPE)
		(stdoutdata, stderrdata) = p.communicate()
		out = []
		for line in stdoutdata.split("\n"):
			if len(line):
				out.append(line)
		return out
	
	def setQueueSlots(self, queueName, slots):
		p = subprocess.Popen(['qconf', '-rattr', 'queue', 'slots', str(slots), queueName], stdout=subprocess.PIPE)
		(stdoutdata, stderrdata) = p.communicate()


            
