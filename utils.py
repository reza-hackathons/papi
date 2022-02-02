from pymongo import MongoClient
import urllib
import requests
import hashlib
import json
from typing import Dict, Any
import os

ENDPOINT = "https://api.thegraph.com/subgraphs/name/perpetual-protocol/perpetual-v2-optimism"

def performRequest(query: str, variables: dict = {}):
  request = requests.post(ENDPOINT,
    json = {'query': query,
            'variables': variables
           },
    headers = {'content-type': 'application/json'}
  )
  if request.status_code == 200:
    return request.json()
  else:
    print('status_code: {}'.format(request.status_code))
    return None

def dict_hash(dictionary: Dict[str, Any]) -> str:
  """MD5 hash of a dictionary."""
  dhash = hashlib.md5()
  # We need to sort arguments so {'a': 1, 'b': 2} is
  # the same as {'b': 2, 'a': 1}
  encoded = json.dumps(dictionary, sort_keys=True).encode()
  dhash.update(encoded)
  return dhash.hexdigest()

def dbInterface(host : str = 'localhost'):
  creds = {}
  creds_path = os.path.join(os.path.dirname(__file__), 'creds.json')
  with open(creds_path) as f:
    creds = json.load(f)['credentials']  
  port = 27017
  user = creds['user']
  pwd  = urllib.parse.quote_plus(creds['pwd'])
  return MongoClient(f'mongodb://{user}:{pwd}@{host}:{port}')