from dbclient import *

# run the query using the query runner
def run_query(url, token, q, cname):

  print("Starting query runner ...")
  q_client = QueryClient(token, url)

  return q_client.run_sql(q, cname)


def lambda_handler(event, context):
  # set up a dictionary of environments if calling across environments
  envs = {}
  envs['myenv'] = ('https://MYENV.cloud.databricks.com', 'my_token')

  # config the cluster name to connect to
  cluster_name = "test_cluster"
  # grab the connection properties
  conn = envs['myenv']
  # define a query to run
  query = """ select count(1) from mwc.companies where country = '{0}' limit 50 """.format('US')
  # run the query and print the results
  response = run_query(conn[0], conn[1], query, cluster_name)

  # return the query output 
  message = "Completed the query!"
  return { 
      'message' : response
  }

