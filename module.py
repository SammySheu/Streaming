import redis
import traceback
import time
import json
import random
from multithread import RestartableThread
from typing import Callable
from redis.client import PubSub
from schemas import CommandPackage
from encryption import Encryption
import queue
import os



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
    # data["data"] = json.loads(data["data"])
    return data

def redis_xrange_to_python(data):
    channel_name, _dl = data[0]
    result = []
    _temp = {k.decode(): v.decode() for k, v in _dl.items()}
    result.append(_temp)
    return result

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
        # self.pid = os.getpid()
        self.data_streaming = queue.Queue()
        self.run_command: Callable = None
        self.subscribe_topics: str = "ResolvedChannel"
        self.resolved = 0
        self.consumers_loading: dict = {}
        '''
        {
            consumer: int
        }
        '''
        # Class init with redis and consumer group
        self.__init_redis()
        self.__create_xgroup(user)
        self.__build_stream_thread(consumer_group=user, consumer=self.myself)
        self.__build_pending_thread()
        self.__build_working_thread()
        self.__build_pubsub_thread()

    def __init_redis(self) -> None:
        self.redis_server = redis.Redis(host=self.redis_host, port = self.redis_port, db=self.redis_db)

    def __create_xgroup(self, consumer_group) -> None:
        if not self.redis_server.exists(consumer_group):
            try:
                self.redis_server.xgroup_create(name=self.channel_name, groupname=consumer_group, mkstream=True)
            except Exception:
                pass
        self.myself = f"CG_{os.getpid()}"
        self.consumer_loading = {self.myself: 0}
        self.look_up_consumers()
    
    def add_message(self, message: dict) -> None:
        self.redis_server.xadd(name=self.channel_name, fields=message)
    
    def message_confirm_and_ack_delete(self, id: str):
        self.redis_server.xack(self.channel_name, self.user, id)
        self.redis_server.xdel(self.channel_name, id)
        # print(f"Resolved case: {self.resolved}")

    def exec_cmd(self, cmd_package_list: list[dict]) -> None:
        if not random.randint(0, 1):
            raise Exception
        for _cmd in cmd_package_list:
            self.resolved += 1
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
                    _d["token"] = Encryption.build_token()
                    self.command_token.add(_d["token"])
                try:
                    self.exec_cmd(cmd_package_list=_data_list)
                    # print("data_received_from_queue")
                except:
                    pass
    def __build_working_thread(self):
        self.working_thread = RestartableThread(target=self.process_queuing_data, args=(self.data_streaming, ))
        self.working_thread.start()
    
    def __channel_listening(self, topics) -> PubSub:
        _redis_instance = self.redis_server.pubsub()
        _redis_instance.subscribe(topics)
        return _redis_instance

    def pubsub_listening(self, pubsub: PubSub):
        for i in pubsub.listen():
            if i["type"] == "message":
                data = redis_subscribe_to_python(i)
                if data["token"] in self.command_token:
                    self.count += 1
                    # print(self.count)
                    self.message_confirm_and_ack_delete(id=data.get("id"))
                # print(f"in sub channel: {data}")

    def __build_pubsub_thread(self):
        _instance = self.__channel_listening(self.subscribe_topics)
        self.subpub_thread = RestartableThread(target=self.pubsub_listening, args=(_instance, ))
        self.subpub_thread.start()

    def put_message_to_working_thread(self, data: dict) -> None:
        try:
            self.data_streaming.put(data)
        except TypeError:
            traceback.print_exc()

    def read_data_in_stream(self, stream_id: str):
        data = self.redis_server.xrange(self.channel_name, min=stream_id, max=stream_id)
        return redis_xrange_to_python(data)

    def __update_consumer_loading(self):
        pending_info = self.redis_server.xpending(name=self.channel_name, groupname=self.user)
        for _consumer in pending_info["consumers"]:
            self.consumer_loading[_consumer["name"].decode()] = _consumer["pending"]

    def __elect_master(self) -> str:
        if self.consumer_loading[self.myself] == 0:
            return self.myself
        else:
            temp: str = self.myself
            for _name, _pending in self.consumer_loading.items():
                if _pending < self.consumer_loading[temp]:
                    temp = _name
            return temp
    def process_pending_list(self):
        i = 0
        def inner_function():
            
            nonlocal i
            # print("PROCESS!!")
            self.__update_consumer_loading()
            # Choose master which has the minimum pending messages
            master = self.__elect_master()
            # print(f"")
            pending_list = self.redis_server.xpending_range(
                name=self.channel_name,
                groupname=self.user,
                consumername=self.myself,
                min="-",
                max="+",
                count=1000)
            print(f"{i} PEL:{len(pending_list)} myself={self.myself} master={master}")
            i += 1
            for _p_info in pending_list:
                # Only dealing with messages that are belong to itself and stay there surpass 3 seconds
                if _p_info["time_since_delivered"] > 3000:
                    data = self.read_data_in_stream(_p_info["message_id"].decode())
                    for _d in data:
                        _d["id"] = _p_info["message_id"].decode()
                        _d["token"] = Encryption.build_token()
                        self.command_token.add(_d["token"])
                    try:
                        self.exec_cmd(cmd_package_list=data)
                        # print("data_received_from_queue")
                    except:
                        pass
                
            # claim other messages if idle time is greater than 5 seconds and not belong to itself
            self.message_autoclaim_to_master(master=master)
        # while True:
        #     schedule.every(3).seconds.do(inner_function) 
        while True:
            inner_function()
            time.sleep(3)


    def message_autoclaim_to_master(self, master: str):
        self.redis_server.xautoclaim(name=self.channel_name, groupname=self.user, consumername=master, min_idle_time=10000)
        # print(result)

    def look_up_consumers(self):
        groups = self.redis_server.xinfo_consumers(self.channel_name, self.user)
        for _c in groups:
            self.consumer_loading[_c.get("name").decode()] = 0
    
    def stream_listening(self, consumer_group: str, consumer: str, block: bool = False):
        while True:
            data = self.redis_server.xreadgroup(
                groupname=consumer_group,
                consumername=consumer,
                streams={
                    self.channel_name: ">"
                },
                block=0 if block else None)
            if data:
                result = redis_xread_to_python(data)
                self.put_message_to_working_thread(result)
            # Main process and working thread share the same memory(self.data_streaming)

    def __build_pending_thread(self):
        self.pending_thread = RestartableThread(target=self.process_pending_list)
        self.pending_thread.start()

    def __build_stream_thread(self, consumer_group: str, consumer: str, block: bool = False):
        # self.consumers[consumer] = {}
        self.myself = consumer
        # Once get the message, back to the starting point to read message
        self.main_thread = RestartableThread(target=self.stream_listening, args=(consumer_group, consumer, True))
        self.main_thread.start()

if __name__ == "__main__":
    cb = Streaming(user="CG", redis_host="127.0.0.1", redis_port=6379, redis_db=13, channel_name="NCB")
    # n = 0
    # while n<2: 
    #     print("PROCESS!!")
    #     cb.process_pending_list()
    #     time.sleep(1)
        # n += 1
    # cb.stream_connect(consumer_group="CG", consumer=f"CG_{cb.pid}", block=True)
    # print(cb.consumers)
    # cb.look_up_consumers()
    

    # cb = Streaming(user="CG", redis_host="127.0.0.1", redis_port=6379, redis_db=13, channel_name="NCB")
    # from schemas import CommandPackage
    # import json
    # for i in range(100):
    #     _temp = CommandPackage(data=json.dumps({i: "world"}))
    #     cb.add_message(message=_temp.model_dump())