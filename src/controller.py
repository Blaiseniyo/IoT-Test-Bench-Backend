from flask_restful import Resource, abort
from src import api
from src.services import MqttService
from src.utils.validations import connection_args,test_broker_args

class Test_Connection(Resource):
    def post(self):
        body = connection_args.parse_args()
        res=MqttService.connection(body["host"],body["port"],body["username"],body["password"])
        if res["connected"]:
            res["client"].disconnect()
            return {"message":"connected successfuly"}, 200
        else:
            abort(401)

class Test_Broker(Resource):
    def post(self):
        body = test_broker_args.parse_args()
        result = MqttService.test(body)
        if result["connection_status"]:
            return result, 200
        return result, 401
            


api.add_resource(Test_Connection,"/connect")
api.add_resource(Test_Broker,"/test")



