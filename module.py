import redis
import traceback
import time
import json
import random
from multithread import RestartableThread
from typing import Callable
from redis.client import PubSub
from schemas import CommandPackage
import queue
import os
import base64
import hmac
import hashlib

def redis_xread_to_python(data) -> list:
    channel_name, _dl = data[0]
    result = []
    for _d in _dl:
        id, td = _d
        _temp = {k.decode(): v.decode() for k, v in td.items()}
        _temp["id"] = id.decode()
        result.append(_temp)
    return result

def redis_subscribe_to_python(data):
    data = json.loads(data["data"].decode())
    data["data"] = json.loads(data["data"])
    return data

class Streaming():
    '''
    channel_name == stream in redis
    asynchronous == whether to wait for reply
    '''
    def __init__(self,
        user: str,
        redis_host: str,
        redis_port: int,
        redis_db: int,
        channel_name: str,
        asynchronous: bool = False
                    ) -> None:
        self.user = user
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.channel_name = channel_name # stream in another word
        self.asynchronous = asynchronous 
        self.count = 0
        self.command_token = set()
        self.pid = os.getpid()
        self.data_streaming = queue.Queue()
        self.run_command: Callable = None
        self.subscribe_topics: str = "ResolvedChannel"
        # Class init with redis and consumer group
        self.__init_redis()
        self.__create_xgroup(user)
        self.__build_working_thread()
        self.__build_pubsub_thread()

    def __init_redis(self) -> None:
        self.redis_server = redis.Redis(host=self.redis_host, port = self.redis_port, db=self.redis_db)

    def __create_xgroup(self, consumer_group) -> None:
        if not self.redis_server.exists(self.channel_name):
            try:
                self.redis_server.xgroup_create(name=self.channel_name, groupname=consumer_group, mkstream=True)
            except Exception:
                pass
    
    def __b64_encode_decode(self, target: str or bytes, command: str or list, en_de: bool = False) -> str:
        run_dict = {
            "85": [base64.b85encode, base64.b85decode],
            "64": [base64.b64encode, base64.b64decode],
            "32": [base64.b32encode, base64.b32decode],
            "16": [base64.b16encode, base64.b16decode],
        }

        if isinstance(target, str):
            target = bytes(target, "utf-8")
        if isinstance(command, list):
            for i in command:
                target = self.__b64_encode_decode(target, i, en_de)
        else:
            if isinstance(command, (int, float)):
                command = str(command).split(".")[0]
            if command not in run_dict.keys():
                return target

            target = run_dict[command][en_de](target)

        if isinstance(target, bytes):
            target = target.decode("utf-8")

        return target

    def add_message(self, message: dict) -> None:
        self.redis_server.xadd(name=self.channel_name, fields=message)
    
    def message_confirm_and_ack_delete(self, id: str):
        self.redis_server.xack(self.channel_name, self.user, id)
        self.redis_server.xdel(self.channel_name, id)

    def exec_cmd(self, cmd_package_list: list[dict]) -> None:
        if not random.randint(0, 50):
            raise Exception
        for _cmd in cmd_package_list:
            self.redis_server.publish(self.subscribe_topics, json.dumps(_cmd))

    def process_queuing_data(self, data_queue: queue.Queue):
        while True:
            _data_list = data_queue.get()
            if _data_list is not None:
                '''
                    Decide whether data need to be executed
                    (Store in self.command)
                '''
                for _d in _data_list:
                    _d["token"] = self.build_token()
                    self.command_token.add(_d["token"])
                self.exec_cmd(cmd_package_list=_data_list)
                # print("data_received_from_queue")

    def __build_working_thread(self):
        self.working_thread = RestartableThread(target=self.process_queuing_data, args=(self.data_streaming, ))
        self.working_thread.start()
    
    def __channel_listening(self, topics) -> PubSub:
        _redis_instance = self.redis_server.pubsub()
        _redis_instance.subscribe(topics)
        return _redis_instance
    
    def __bs_hmc_encode(self, password: str, sk: str) -> str:
        sk = bytes(sk, 'utf-8')
        password = bytes(password, 'utf-8')
        signature_hash = hmac.new(sk, password, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(signature_hash).decode()
        return signature

    def __token_generator(self, account: str, password: str, sk: str) -> str:
        signature = self.__bs_hmc_encode(password, sk)
        token = "{},{},{}".format(signature, account, time.time() + 300)
        token = self.__b64_encode_decode(target=token, command=[85, 64])
        return token

    def build_token(self) -> str:
        result = self.__token_generator("NormalTA", "0", str(time.time()))
        return result

    def pubsub_listening(self, pubsub: PubSub):
        for i in pubsub.listen():
            if i["type"] == "message":
                data = redis_subscribe_to_python(i)
                if data["token"] in self.command_token:
                    self.count += 1
                    print(self.count)
                    self.message_confirm_and_ack_delete(id=data.get("id"))
                # print(f"in sub channel: {data}")

    def __build_pubsub_thread(self):
        _instance = self.__channel_listening(self.subscribe_topics)
        self.subpub_thread = RestartableThread(target=self.pubsub_listening, args=(_instance, ))
        self.subpub_thread.start()

    def put_message_to_working_thread(self, data: dict) -> None:
        try:
            self.data_streaming.put(data)
            # self.job_count += 1
            # self.run_command(command_script=command, data=_data.dict())
            # self.redis_stream_ack_and_del(_channel, _id)
            # self.insert_to_influx_db_for_stream(_data, _id, "finish command", _channel)
        except TypeError:
            traceback.print_exc()


    def stream_connect(self, consumer_group: str, consumer: str, block: bool = False):
        # Once get the message, back to the starting point to read message
        while True:
            data = self.redis_server.xreadgroup(
                groupname=consumer_group,
                consumername=consumer,
                streams={
                    self.channel_name: ">"
                },
                block=0 if block else None)
            result = redis_xread_to_python(data)
            self.put_message_to_working_thread(result)
            # Main process and working thread share the same memory(self.data_streaming)
            
            # Call command here with async
            # Async function should be wrapped in __enter__ and __exit__ function()

if __name__ == "__main__":
    cb = Streaming(user="CG", redis_host="127.0.0.1", redis_port=6379, redis_db=13, channel_name="NCB")
    cb.stream_connect(consumer_group="CG", consumer=f"CG_{cb.pid}", block=True)
    

    # cb = Streaming(user="CG", redis_host="127.0.0.1", redis_port=6379, redis_db=13, channel_name="NCB")
    # from schemas import CommandPackage
    # import json
    # for i in range(100):
    #     _temp = CommandPackage(data=json.dumps({i: "world"}))
    #     cb.add_message(message=_temp.model_dump())