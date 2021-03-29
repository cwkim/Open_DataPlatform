# -*- coding: utf-8 -*-
# Author : jeonghoonkang , https://github.com/jeonghoonkang

import urllib

url = "http://125.140.110.217:54242/q?"
_url = url + "start=" + '2019/06/01-00:00:00' + "&end=" + '2019/06/08-00:00:00'
rqst_url = _url + '&m=none:Elex_data&o=&key=out%20bottom%20center&wxh=800x600&style=points&png'
print (rqst_url)
urllib.urlretrieve(rqst_url, "down-file.jpg")

#http://125.140.110.217:54242/q?
#start=2019/06/01-00:00:00
#&end=2019/06/08-00:00:00
#&m=none:Elex_data
#&o=&key=out%20bottom%20center&wxh=800x600&style=points&png