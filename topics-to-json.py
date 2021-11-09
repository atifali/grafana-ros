#!/usr/bin/env python3
import os
import sys
import rospy
from importlib import import_module
import yaml
import json
import requests

api_token = ""

class GenericMessageSubscriber(object):
    def __init__(self, topic_name, callback):
        self._binary_sub = rospy.Subscriber(
            topic_name, rospy.AnyMsg, self.generic_message_callback)
        self._callback = callback
        self._topic_name = topic_name

    def generic_message_callback(self, data):
        assert sys.version_info >= (2,7) #import_module's syntax needs 2.7
        connection_header =  data._connection_header['type'].split('/')
        ros_pkg = connection_header[0] + '.msg'
        msg_type = connection_header[1]
        msg_class = getattr(import_module(ros_pkg), msg_type)
        #try:
            # some msg types don't have the _buff attribute attached!
        msg = msg_class().deserialize(data._buff)
        self._callback(msg, self._topic_name)
        #except:
        #    print("ERROR: Could not publish for topic: " + self._topic_name)

def msg2json(msg, topic):
    yaml_msg = yaml.load(str(msg), Loader=yaml.FullLoader)
    json_msg = json.dumps(yaml_msg,indent=4)
    url = "http://localhost:3000/api/live/push/ros" + topic
    headers = {'Authorization': 'Bearer ' + api_token}
    response = requests.post(url=url, json=json.loads(json_msg), headers=headers, allow_redirects=False)
    if response.status_code != 200:
        print("ERROR! TOPIC: " + topic + "; CODE: " + str(response.status_code))

def publishAllTopics():
    topics = rospy.get_published_topics("/")
    threads = []
    print("found the following topics...")
    for topic in topics:
        print(topic)
        thread = Thread(target = GenericMessageSubscriber, args= (topic[0], msg2json))
        threads.append(thread)
        thread.start()        
    rospy.spin()
    for t in threads:
        t.join()

def main():
    global api_token
    api_token = str(os.getenv('GF_TOKEN'))
    rospy.init_node('ros2json')
    GenericMessageSubscriber("/imu", msg2json)
    GenericMessageSubscriber("/odom", msg2json)
    GenericMessageSubscriber("/cmd_vel", msg2json)
    GenericMessageSubscriber("/joint_states", msg2json)
    GenericMessageSubscriber("/tf", msg2json)
    rospy.spin()    

if __name__ == '__main__':
    main()
