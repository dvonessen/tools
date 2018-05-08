import requests as req
from time import sleep
import sys

bust_url = ''
bust_range = 100000  # Count of request to target
bust_sleep = 50   # Sleep time in ms

for bust in range(0, bust_range):
    try:
        str_bust = str(bust)
        req_url = bust_url
        req_url += "?u=" + str_bust
        req_url += "&ac=" + str_bust
        req_url += "&as=" + str_bust
        res = req.get(req_url)
    except req.exceptions.MissingSchema as ex:
        print("bust_url must contain schema.\n One of http:// or https://")
        sys.exit(127)
    else:
        print("Bust Request: {}".format(req_url))
        print("Bust Response: {}".format(res.json()))
        sleep(bust_sleep/1000)
