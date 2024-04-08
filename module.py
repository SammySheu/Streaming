import redis
import traceback
import time
import json
import random
from multithread import RestartableThread
from typing import Callable
from redis.client import PubSub
from schemas import CommandPackage, ConsumerInfo
from encryption import Encryption
import queue
import os

def redis_xread_to_python(data) -> list:
    try:
        channel_name, _dl = data[0]
        result = []
        for _d in _dl:
            id, td = _d
            _temp = {k.decode(): v.decode() for k, v in td.items()}
            _temp["id"] = id.decode()
            result.append(_temp)
    except:
        print(data)
    return result

def redis_subscribe_to_python(data):
    data = json.loads(data["data"].decode())
    # data["data"] = json.loads(data["data"])
    return data

def redis_xrange_to_python(data):
    try:
        channel_name, _dl = data[0]
        result = []
        _temp = {k.decode(): v.decode() for k, v in _dl.items()}
        result.append(_temp)
    except:
        print(data)
    return result

class Streaming():
    '''
    channel_name == stream in redis
    asynchronous == whether to wait for reply
    '''
    def __init__(self,
        user_module: str,
        redis_host: str,
        redis_port: int,
        redis_db: int,
        channel_name: str,
        asynchronous: bool = False
                    ) -> None:
        self.group = user_module
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.channel_name = channel_name # stream in another word
        self.asynchronous = asynchronous 
        self.count = 0
        self.command_token = set()
        self.data_streaming = queue.Queue()
        self.run_command: Callable = None
        self.subscribe_topics: str = "ResolvedChannel"
        self.resolved = 0
        self.time = int(time.time())
        # Class init with redis and consumer group
        self.__init_redis()
        self.__create_xgroup(user_module)
        self.__build_stream_thread(consumer_group=user_module, consumer=self.workerID)
        self.__build_working_thread()
        self.__build_pubsub_thread()
        self.__build_pending_thread()

    def __init_redis(self) -> None:
        self.redis_server = redis.Redis(host=self.redis_host, port = self.redis_port, db=self.redis_db)

    def __create_xgroup(self, consumer_group) -> None:
        if not self.redis_server.exists(consumer_group):
            try:
                self.redis_server.xgroup_create(name=self.channel_name, groupname=consumer_group, mkstream=True)
            except Exception:
                pass
        self.workerID = f"{self.group}_{os.getpid()}"

    def __look_up_consumers(self) -> dict[str, ConsumerInfo]:
        """
        Return info of consumers
        """
        groups = self.redis_server.xinfo_consumers(self.channel_name, self.group)
        info = { _c["name"].decode(): ConsumerInfo(**_c) for _c in groups }
        return info

    def __elect_master(self, info: dict[str, ConsumerInfo]) -> str:
        """
        Using info of consumers to elect master
        """
        master, current = self.workerID, info[self.workerID].pending
        if current == 0:
            return master
        for _name, _info in info.items():
            if _info.pending == 0:
                return _name
            elif _info.pending < current:
                master, current = _name, _info.pending
        return master
    
    def __delete_inactive_consumers(self, consumers_info: dict[str, ConsumerInfo], dead_time: int = 60000) -> dict[str, ConsumerInfo]:
        _to_delete = set()
        for _consumer, _info in consumers_info.items():
            if _info.idle > dead_time:
                self.redis_server.xgroup_delconsumer(name=self.channel_name, groupname=self.group, consumername=_consumer)
                _to_delete.add(_consumer)
        for _t in _to_delete:
            del consumers_info[_t]
    
    def message_add(self, message: dict) -> None:
        self.redis_server.xadd(name=self.channel_name, fields=message)

    def message_autoclaim_to_master(self, master: str, idle_time: int = 60000):
        """
        Auto-claim messages to master if idle time surpassed
        """
        self.redis_server.xautoclaim(name=self.channel_name, groupname=self.group, consumername=master, min_idle_time=idle_time)
    
    def message_confirm_and_ack_delete(self, id: str):
        self.redis_server.xack(self.channel_name, self.group, id)
        self.redis_server.xdel(self.channel_name, id)
        print(f"Resolved case: {self.resolved}")
        print(int(time.time()) - self.time)

    def channel_subscribe(self, topics) -> PubSub:
        _redis_instance = self.redis_server.pubsub()
        _redis_instance.subscribe(topics)
        return _redis_instance
    
    def put_message_to_working_thread(self, data: dict) -> None:
        try:
            self.data_streaming.put(data)
        except TypeError:
            traceback.print_exc()

    def read_data_in_stream(self, stream_id: str):
        data = self.redis_server.xrange(self.channel_name, min=stream_id, max=stream_id)
        return redis_xrange_to_python(data)

    def exec_cmd(self, cmd_package_list: list[dict]) -> None:
        # if not random.randint(0, 1):
        #     raise Exception
        for _cmd in cmd_package_list:
            self.resolved += 1
            self.redis_server.publish(self.subscribe_topics, json.dumps(_cmd))

    def queuing_data_processing(self, data_queue: queue.Queue):
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
                except:
                    pass
            data_queue.task_done()

    def pubsub_listening(self, pubsub: PubSub):
        for i in pubsub.listen():
            if i["type"] == "message":
                data = redis_subscribe_to_python(i)
                if data["token"] in self.command_token:
                    self.count += 1
                    # print(self.count)
                    self.message_confirm_and_ack_delete(id=data.get("id"))
                # print(f"in sub channel: {data}")

    def pending_list_processing(self):
        while True:
            info = self.__look_up_consumers()
            self.__delete_inactive_consumers(info)
            # Choose master which has the minimum pending messages
            masterID = self.__elect_master(info)
            # Process self's pending list
            pending_list = self.redis_server.xpending_range(
                name=self.channel_name,
                groupname=self.group,
                consumername=self.workerID,
                min="-",
                max="+",
                count=1000)
            self.redis_server.xpending(name="NCB", groupname="Module")
            # print(f"PEL:{len(pending_list)} myself={self.workerID} master={masterID}")
            for _p_info in pending_list:
                # Only dealing with messages that are belong to itself and stay there surpass 3 seconds
                if _p_info["time_since_delivered"] > 60000:
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
            self.message_autoclaim_to_master(master=masterID)
            # time.sleep(5)

    def stream_listening(self, consumer_group: str, consumer: str, block: bool = False):
        while True:
            data = self.redis_server.xreadgroup(
                groupname=consumer_group,
                consumername=consumer,
                streams={
                    self.channel_name: ">"
                },
                count=100,
                block=0 if block else None)
            if data:
                result = redis_xread_to_python(data)
                self.put_message_to_working_thread(result)
            self.data_streaming.join()

    def __build_stream_thread(self, consumer_group: str, consumer: str, block: bool = False):
        self.workerID = consumer
        # Once get the message, back to the starting point to read message
        self.main_thread = RestartableThread(target=self.stream_listening, args=(consumer_group, consumer, True))
        self.main_thread.start()

    def __build_working_thread(self):
        self.working_thread = RestartableThread(target=self.queuing_data_processing, args=(self.data_streaming, ))
        self.working_thread.start()

    def __build_pubsub_thread(self):
        _instance = self.channel_subscribe(self.subscribe_topics)
        self.subpub_thread = RestartableThread(target=self.pubsub_listening, args=(_instance, ))
        self.subpub_thread.start()
    
    def __build_pending_thread(self):
        self.pending_thread = RestartableThread(target=self.pending_list_processing)
        self.pending_thread.start()

if __name__ == "__main__":
    cb = Streaming(user_module="Module", redis_host="127.0.0.1", redis_port=6379, redis_db=13, channel_name="NCB")