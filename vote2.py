# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 00:08:52 2018

@author: yipin
"""


import requests
import pandas as pd
from bs4 import BeautifulSoup
import lxml
import bs4
import json
import csv
import base64
from datetime import datetime, date, time
import requests
import pytz

"""Visegrad+ parliament API client module.
Contains functions for sending API requests conveniently.
"""

__all__ = [
    'parliament', 'authorize', 'deauthorize',
    'get', 'getall', 'getfirst', 'post', 'put', 'patch', 'delete',
    'timezone', 'utc_to_local', 'local_to_utc',
]

SERVER_NAME = 'api.parldata.eu'
SERVER_CERT = 'server_cert.pem'
PARLIAMENT = ''
LOCAL_TIMEZONE = None
PAYLOAD_HEADERS = {
    'Content-Type': 'application/json',
}


def _endpoint(method, resource, id=None):
    """Returns URL of the resource or its item for the given method.
    http:// is used for GET method while https:// for the others.
    http:// is used for all methods on localhost.
    """
    if method=='GET' or SERVER_NAME.startswith('localhost:') or SERVER_NAME.startswith('127.0.0.1:'):
        protocol = 'http'
    else:
        protocol = 'https'
    if PARLIAMENT:
        url = '%s://%s/%s/%s' % (protocol, SERVER_NAME, PARLIAMENT, resource)
    else:
        url = '%s://%s/%s' % (protocol, SERVER_NAME, resource)
    if id is not None:
        url = '%s/%s' % (url, id)
    return url


def _jsonify_dict_values(params):
    """Returns `params` dictionary with all values of type dictionary
    or list serialized to JSON. This is necessary for _requests_
    library to pass parameters in the query string correctly.
    """
    return {k: json.dumps(v) if isinstance(v, dict) or isinstance(v, list) else v for k, v in params.items()}


def parliament(parl=None):
    """Sets the parliament the following requests will be sent to.
    Returns previous, now overwritten value.
    Used without arguments returns current value without any change.
    """
    global PARLIAMENT
    old = PARLIAMENT
    if parl is not None:
        PARLIAMENT = parl
    return old


def authorize(username, password):
    """Sets API username and password for the following data modifying
    requests.
    """
    s = '%s:%s' % (username, password)
    PAYLOAD_HEADERS['Authorization'] = b'Basic ' + base64.b64encode(s.encode('ascii'))


def deauthorize():
    """Unsets API username and password - the following data modifying
    requests will be anonymous.
    """
    PAYLOAD_HEADERS.pop('Authorization', None)


def get(resource, id=None, **kwargs):
    """Makes a GET (read) request on the resource or specific item.
    Lookup or other parameters are specified as keyword arguments.
    """
    resp = requests.get(
        _endpoint('GET', resource, id),
        params=_jsonify_dict_values(kwargs),
        verify=SERVER_CERT
    )
    resp.raise_for_status()
    return resp.json()


def getall(resource, **kwargs):
    """Generator that generates sequence of all found results
    without paging. Lookup parameters are specified as keyword
    arguments.

    Usage:
        items = vpapi.getall(resource, where={...})
        for i in items:
         ...
    """
    page = 1
    while True:
        resp = get(resource, page=page, **kwargs)
        for item in resp['_items']:
            yield item
        if 'next' not in resp['_links']: break
        page += 1

if __name__ == "__main__":
    country = 'cz'
    party = 'senat'
    parliament(country+'/'+party)
    vote_test = get('votes')
    df = pd.DataFrame(vote_test['_items'])
    #endtime = time.time()
    df.to_csv(country+'_'+party+'_vote_test.csv', encoding = 'utf-8-sig',index = False)
