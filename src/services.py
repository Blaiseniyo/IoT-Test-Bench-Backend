from src.utils.BrokerUtils import Broker

class MqttService:
    def test(body):
        return Broker.run(Broker,body)
    
    def connection(host,port,username,password):
        return Broker.check_connection(host,port,username,password)