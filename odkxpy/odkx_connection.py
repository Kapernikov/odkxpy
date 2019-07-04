import requests
import json
import datetime
import logging


class OdkxConnection(object):
    """
    TODO : implement client version support
    """
    def __init__(self, server, user, pwd, proxies=None, appID="default"):
        self.user = user
        self.pwd = pwd
        self.appID = appID
        self.server = server
        self.proxies = proxies
        self.session = requests.session()
        self.session.proxies = proxies
        self.session.auth = (self.user, self.pwd)

    def treatResponse(self, response):
        logging.debug("HTTP status: \033[92m[" + str(response.status_code) + ']\033[0m - ' + response.url)
        if (response.status_code == 200) and response.text:
            output = response.json()
        elif response.status_code == 200:
            output = response
        else:
            output = response.content
            raise Exception("HTTP {code} {content}".format(code=response.status_code, content=output))
        return output

    def PUT(self, url, data_):
        """alter tables through HTTP PUT
        """
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps(data_)
        response = self.session.put(self.server+self.appID+'/'+url, headers=headers, data=payload)
        return self.treatResponse(response)

    def GET(self, url, params=None):
        """fetch tables through HTTP GET
        """
        response = self.session.get(self.server+self.appID+'/'+url, params=params)
        return self.treatResponse(response)

    def POST(self, url, data, headers=None):
        h_= {}
        if headers:
            h.update(headers)
        response = self.session.post(self.server + self.appID + '/' + url
            , headers=headers,
            data=data)
        return self.treatResponse(response)

    def DELETE(self, url):
        return self.treatResponse(self.session.delete(self.server + self.appID+'/'+url))

