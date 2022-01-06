from flask_restful import Resource, abort
from src import api
from src.services import MqttService
import paho.mqtt.client as mqtt
from src.utils.validations import connection_args, test_broker_args


class Test_Connection(Resource):
    def post(self):
        body = connection_args.parse_args()
        res = MqttService.connection(
            body["host"], body["port"], body["username"], body["password"])
        if res["connected"]:
            res["client"].disconnect()
            return {"message": "connected successfuly"}, 200
        else:
            # abort(401)
            return {"message": "can not connect to the broker"}, 401


class Test_Publisher(Resource):
    def post(self):
        try:
            body = connection_args.parse_args()
            topic = 'test/test1'
            msg = 'this is a test message'
            client = mqtt.Client("P1")
            client.connect(body['host'], body['port'])
            result = client.publish(topic,msg, 1, False)
            client.disconnect()
            print(result[0])
            return {'message': "publisher connection succeeded"}, 200
        except:
            return {'message': "Publisher connection failed"}, 401


class Test_Subscriber(Resource):
    def post(self):
        try:
            body = connection_args.parse_args()
            topic = "test/test1"
            client = mqtt.Client("P1")
            client.connect(body['host'], body['port'])
            result = client.subscribe(topic)
            client.disconnect()
            print(result[0])
            return {'message': "Subscriber connection succeeded"}, 200
        except:
            return {'message': "Subscriber connection failed"}, 401
        


class Test_Broker(Resource):
    def post(self):
        body = test_broker_args.parse_args()
        result = MqttService.test(body)
        if result["connection_status"]:
            return result, 200
        return result, 401


api.add_resource(Test_Connection, "/connect")
api.add_resource(Test_Publisher, "/publisher/connect")
api.add_resource(Test_Subscriber, "/subscriber/connect")
api.add_resource(Test_Broker, "/test")
