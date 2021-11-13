#!/usr/bin/env python3
import os
import sys
import rospy
from importlib import import_module
import yaml
import json
import requests
from multiprocessing import Process
import websocket
from rosthrottle import MessageThrottle

API_TOKEN = ""
THROTTLING_RATE = 10.0

class GenericMessageSubscriber(object):
    def __init__(self, topic_name, callback, use_ws, use_throttling):
        self._topic_name = topic_name
        self._url = "http://localhost:3000/api/live/pipeline/push/stream/ros" + self._topic_name
        self._headers = {'Authorization': 'Bearer ' + API_TOKEN}
        self._use_throttling = use_throttling
        if self._use_throttling:
            self._throttler = MessageThrottle(self._topic_name, self._topic_name + "_t", THROTTLING_RATE)
            self._topic_name = self._topic_name + "_t"
            self._throttler.start()
        else:
            self._throttler = None
        self._binary_sub = rospy.Subscriber(
            self._topic_name, rospy.AnyMsg, self.generic_message_callback)
        self._callback = callback
        self._use_ws = use_ws
        if self._use_ws:
            self._url = self._url.replace("http", "ws")
            self._ws = websocket.WebSocketApp(self._url, header=self._headers)
            self._ws.run_forever(skip_utf8_validation=True)
        else:
            self._ws = None

    def generic_message_callback(self, data):
        assert sys.version_info >= (2,7) #import_module's syntax needs 2.7
        connection_header =  data._connection_header['type'].split('/')
        ros_pkg = connection_header[0] + '.msg'
        msg_type = connection_header[1]
        msg_class = getattr(import_module(ros_pkg), msg_type)
        try:
            # some msg types don't have the _buff attribute attached!
            if hasattr(data, '_buff'):
                msg = msg_class().deserialize(data._buff)
            # try using the object itself...
            else:
                msg = data
        except Exception as e:
            print("ERROR: Could not deserialize message for topic: " + self._topic_name)
            print(e)
        self._callback(msg, self._url, self._headers, self._ws, self._use_ws, self._topic_name)

def msg2json(msg, url, headers, ws, use_ws, topic):
    yaml_msg = yaml.load(str(msg), Loader=yaml.FullLoader)
    json_msg = json.dumps(yaml_msg,indent=4)
    if use_ws:
        try:
            ws.send(json_msg)
        except Exception as e:
            print("ERROR! TOPIC: " + topic + "; WS SEND FAILED!")
            print(e)
    else:
        response = requests.post(url=url, json=json.loads(json_msg), headers=headers, allow_redirects=False)
        if response.status_code != 200:
            print("ERROR! TOPIC: " + topic + "; CODE: " + str(response.status_code))

def publishTopic(topic, use_ws, use_throttling):
    rospy.set_param('enable_statistics', True)
    rospy.set_param('statistics_window_min_elements', 10)
    rospy.init_node('ros2json' + topic.replace('/', '000').replace('_', '000'))
    GenericMessageSubscriber(topic, msg2json, use_ws, use_throttling)
    rospy.spin()
    rospy.signal_shutdown("program exiting")

def publishAllTopics(use_ws, use_throttling):
    topics = rospy.get_published_topics("/")
    processes = []
    print("found the following topics...")
    for topic in topics:
        if topic[0].endswith('_t'):
            continue
        print(topic)
        process = Process(target = publishTopic, args= (topic[0], use_ws, use_throttling, ))
        processes.append(process)
        process.start()
    for p in processes:
        p.join()

def main():
    global API_TOKEN
    API_TOKEN = str(os.getenv('GF_TOKEN'))
    publishAllTopics(True, True)

if __name__ == '__main__':
    main()
