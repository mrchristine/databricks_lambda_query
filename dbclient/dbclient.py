import json, requests

global pprint_j
# Helper to pretty print json
def pprint_j(i):
  print(json.dumps(i, indent=4, sort_keys=True))

class dbclient:

  """A class to define wrappers for the REST API"""
  def __init__(self, token="ABCDEFG1234", url="https://myenv.cloud.databricks.com"):
      self._token = {'Authorization': 'Bearer {0}'.format(token)}
      self._url = url

  def get(self, endpoint, json_params = {}, printJson = False, version = '2.0'):
    if version:
      ver = version
    if json_params:
      results = requests.get(self._url + '/api/{0}'.format(ver) + endpoint, headers=self._token, params=json_params).json()
    else: 
      results = requests.get(self._url + '/api/{0}'.format(ver) + endpoint, headers=self._token).json()
    if printJson:
      print(json.dumps(results, indent=4, sort_keys=True))
    return results

  def post(self, endpoint, json_params = {}, printJson = True, version = '2.0'):
    if version:
        ver = version
    if json_params:
      raw_results = requests.post(self._url + '/api/{0}'.format(ver) + endpoint, headers=self._token, json=json_params)
      results = raw_results.json()
    else: 
      print("Must have a payload in json_args param.")
      return {}
    if printJson:
      print(json.dumps(results, indent=4, sort_keys=True))
    # if results are empty, let's return the return status
    if results:
      results['http_status_code'] = raw_results.status_code
      return results
    else:
      return {'http_status_code': raw_results.status_code}

