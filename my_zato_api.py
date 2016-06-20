#-*- coding: utf-8 -*-
from zato.server.service import Service
from urlparse import parse_qs
import json


class GetApis(Service):
    def handle(self):   
        import requests
        response = {}

        """
        发起请求主机地址
        (eg:132.97.135.12:11223 --通常为zato自身)
        """
        HTTP_HOST = self.wsgi_environ['HTTP_HOST']

        """
        发起请求的路径
        (eg:/get_dep_positions --POST请求; 
            /get_dep_positions?name=1&type=2 --GET请求)
        """
        RAW_URI = self.wsgi_environ['RAW_URI']

        """
        发起请求的方法(GET/POST)
        """
        REQUEST_METHOD = self.wsgi_environ['REQUEST_METHOD']
  
        """
        发起请求函数名
        (zato web admin中Connections -> Channels -> Plain HTTP配置的名称,
         即zato暴露出去的方法名称)
        eg:/get_dep_positions
        """
        request_name = self.wsgi_environ['PATH_INFO'].split("/")[1]
        
        """
        ( 保证Connections -> Channels -> Plain HTTP暴露的接口名称 
          与 Connections -> Outgoing -> Plain HTTP 外部接口名称一致 )
        """
        REQUEST_NAME = request_name

        """
        Connections -> Outgoing -> Plain HTTP 中配置的外部接口
        """
        plain_http = self.outgoing.plain_http
        outgoing_https = plain_http.keys()
        if len(outgoing_https) == 0:
            response = {'msg':'zato暂未配置outgoing Plain HTTP 接口！',
                        'flag':False}
            self.response.payload = response
        else:
            if REQUEST_NAME not in outgoing_https:
                response = {'msg':'请求的接口不存在于outgoing Plain HTTP 中(或Channels与Outgoing中的名称不一致)！',
                            'flag':False}
                self.response.payload = response
            else:  
                """
                获取 Connections -> Outgoing -> Plain HTTP 中的相应参数
                """
                conn = plain_http[''+REQUEST_NAME+''].conn

                address_host = conn.config['address_host']
                address_url_path = conn.config['address_url_path']
                outgoing_name = conn.config['name']

                if address_host == '':
                    response = {'msg':'请在outgoing中配置Host！','flag':False}
                    self.response.payload = response
                if address_url_path == '':
                    response = {'msg':'请在outgoing中配置URL path！','flag':False}
                    self.response.payload = response
                if outgoing_name == '':
                    response = {'msg':'请在outgoing中配置Name！','flag':False}
                    self.response.payload = response
                
               
                if REQUEST_METHOD == 'GET':
                    qs = parse_qs(self.wsgi_environ['QUERY_STRING'])
                    """
                    检查是否存在网络问题/或者存在代理问题---待实现
                    """

                    url = address_host + address_url_path
                    http_proxies = {
                        "http": "http://130.51.79.61:3128",
                    }
                    if qs:
                        resp = requests.get(url,proxies=http_proxies,timeout=5.0,params=qs)
                    else:
                        resp = requests.get(url,proxies=http_proxies,timeout=5.0)
                    if resp.status_code != 200:
                        response = {'msg':'请求出错！','flag':False}
                        self.response.payload = response
                    t = resp.text.strip()
                    if t == '':
                        response = {'msg':'原接口返回结果为空！','flag':False}
                        self.response.payload = response
                    self.response.payload = {'msg':'接口调用成功!','raw_r':t,'flag':True}

                elif REQUEST_METHOD == 'POST':
                    qs = self.request.raw_request
                    """
                    检查是否存在网络问题/或者存在代理问题---待实现
                    """
                    
                    url = address_host + address_url_path
                    http_proxies = {
                        "http": "http://130.51.79.61:3128",
                    }

                    if qs:
                        resp = requests.post(url,proxies=http_proxies,timeout=5.0,params=qs)
                    else:
                        resp = requests.post(url,proxies=http_proxies,timeout=5.0)
                    if resp.status_code != 200:
                        response = {'msg':'请求出错！','flag':False}
                        self.response.payload = response
                    t = resp.text.strip()
                    if t == '':
                        response = {'msg':'原接口返回结果为空！','flag':False}
                        self.response.payload = response
                    self.response.payload = {'msg':'接口调用成功!','raw_r':t,'flag':True}
                else:
                    response = {'未知请求类型,目前仅支持GTE/POST请求！'}


