from dbclient import *
import json, time

class QueryClient(dbclient):
  # local attributes to define the execution context id, language, and cluster id
  ec_id = "" 
  language = ""
  cid = ""

  # list spark versions
  def get_spark_versions(self):
    return self.get("/clusters/spark-versions", printJson = True)

  # get cluster list to identify the cluster id
  def get_cluster_list(self, alive = True):
    """ Returns an array of json objects for the running clusters. Grab the cluster_name or cluster_id """
    cl = self.get("/clusters/list", printJson = False)
    if alive:
      running = filter(lambda x: x['state'] == "RUNNING", cl['clusters'])
      for x in running:
        print(x['cluster_name'] + ' : ' + x['cluster_id'])
      return running
    else:
      return cl

  # get the cluster id from cluster name
  def get_cluster_id(self, cname):
    """ Returns the cluster id for a given cluster name. Returns None if cluster name doesn't match """
    cl = self.get_cluster_list()
    for x in cl:
      if x['cluster_name'] == cname:
        return x['cluster_id']
    return None

  def set_execution_context(self, cname, lang):
    self.language = lang
    self.cid = self.get_cluster_id(cname)
    # if the cluster doesn't match, raise an error
    if self.cid is None:
      raise ValueError('Cluster id is null. Cluster name does not match running clusters')

    ec_payload = {"language": self.language, 
                  "clusterId": self.cid}

    ec = self.post('/contexts/create', json_params = ec_payload, version = "1.2")
    # Grab the execution context ID 
    self.ec_id = ec['id']
    print('Execution id : ' + self.ec_id)
    return True

  def run_command(self, cmd):
    # This launches spark commands and print the results. We can pull out the text results from the API
    command_payload = { 'language': self.language, 
                        'contextId' : self.ec_id, 
                        'clusterId' : self.cid, 
                       'command': cmd }
    
    command = self.post('/commands/execute', \
                          json_params = command_payload,\
                          version = "1.2")
    
    com_id = command['id']
    print('command_id : ' + com_id)
    
    result_payload = {'clusterId': self.cid, 
                      'contextId': self.ec_id, 
                      'commandId': com_id} 
    
    resp = self.get('/commands/status', json_params=result_payload, version="1.2")
    is_running = resp['status']
    
    # loop through the status api to check for the 'running' state call and sleep 1 second
    while (is_running == "Running"):
      resp = self.get('/commands/status', json_params=result_payload, version="1.2")
      is_running = resp['status']
      time.sleep(1)
    
    # Print and return results
    print(resp['results'])
    return resp['results']

  def close_context(self):
    # Cleanup of context
    cleanup_payload = {"contextId": self.ec_id, "clusterId": self.cid}
    resp = self.post('/contexts/destroy', json_params = cleanup_payload, version = "1.2")
    
    if resp['http_status_code'] != 200:
      print("Failed to destory context")
    else: 
      print("Sucessful cleanup of context")

  def run_python(self, cmd, cname):
    self.set_execution_context(cname, 'python')
    results = self.run_command(cmd)
    self.close_context()
    return results

  def run_scala(self, cmd, cname):
    self.set_execution_context( cname, 'scala')
    results = self.run_command(cmd)
    self.close_context()
    return results

  def run_sql(self, cmd, cname):
    self.set_execution_context(cname, 'python')
    sql_cmd = """spark.sql(\"{0}\").collect()""".format(cmd)
    results = self.run_command(sql_cmd)
    self.close_context()
    return results


