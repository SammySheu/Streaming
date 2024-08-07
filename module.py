"""
This module provides functionality for streaming data.
"""

import os
import time
import json
import queue
import traceback
from typing import Callable

import redis
from redis import ResponseError
from redis.client import PubSub

from schemas.schemas import CommandPackage, ConsumerInfo, GroupInfo, StreamInfo
from utils.multithread import RestartableThread
from utils.encryption import Encryption
from utils.exception import StreamingException
from utils.stream_operate import StreamOperate
from utils.convert_function import (
    redis_xread_to_python,
    redis_subscribe_to_python,
    redis_xrange_to_python
)


class Streaming():
    '''
    channel_name == stream in redis
    asynchronous == whether to wait for reply
    '''
    command_function = {}

    def __init__(self,
                 user_module: str,
                 redis_host: str,
                 redis_port: int,
                 redis_db: int,
                 receiving_channel_pair: tuple[str, set],
                 sending_channel_pair: tuple[str, set],
                 asynchronous: bool = False
                 ) -> None:
        self.module_name = user_module
        self.module_uid = f"{user_module}_{os.getpid()}"
        self.send_channel = StreamOperate(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            stream_name=sending_channel_pair[0],
            groups=sending_channel_pair[1]
        )
        self.receive_channel = StreamOperate(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            stream_name=receiving_channel_pair[0],
            groups=receiving_channel_pair[1]
        )
        self.redis_server = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db)
        self.asynchronous = asynchronous
        self.count = 0
        self.callback_total = 0
        self.callback_manager = dict()  # {<token>: <thread>}
        self.owned_token = set()
        self.command_data = dict()  # {<token>: <callback_data>}
        self.pid = os.getpid()
        self.data_streaming = queue.Queue()
        self.run_command: Callable = None
        self.subscribe_topics: str = "ResolvedChannel"
        self.listened = 0
        self.resolved = 0
        self.processed = 0
        self.owned_data = 0
        self.process_time = []

        self.time = int(time.time())
        print(
            f"\nDataQueue: {self.owned_data}\n"
            f"\033[1G\033[2KProcessed Data: {self.processed}\n"
            f"\033[1G\033[2KResolved Number: {self.resolved}\n"
            f"\033[1G\033[2KListened Data: {self.listened}",
            end="",
            flush=True
        )

        self.__build_stream_thread(thread_name="StreamListening",
                                   consumer_group=self.module_name,
                                   consumer=self.module_uid,
                                   count=1,
                                   block=True)
        self.__build_working_thread(
            thread_name="QueuingDataProcessing", streaming_data=self.data_streaming)
        self.__build_pubsub_thread(thread_name="BroadcastListening")
        # self.__build_pending_thread(thread_name="PendingListProcessing")
        self.update_register_function()

    @classmethod
    def cmd_register(cls, func: Callable) -> Callable:
        """
        Registers a command function in the command_function dictionary of the given class.
        """
        cls.command_function[func.__name__] = func
        return func

    def update_register_function(self):
        Streaming.command_function.update(self.__register_class_method(self))

    def __look_up_stream(self, channel: str) -> None:
        try:
            _info = self.redis_server.xinfo_stream(name=channel)
            StreamInfo(**_info)
        except ResponseError as e:
            print(e)
            self.redis_server.xgroup_create(
                name=channel,
                groupname=self.module_name,
                mkstream=True)

    def __look_up_group(self, channel: str) -> dict[str, GroupInfo]:
        try:
            _groups = self.redis_server.xinfo_groups(name=channel)
        except ResponseError:
            return {}
        return {_g["name"].decode(): GroupInfo(**_g) for _g in _groups}

    def __look_up_consumers(self, channel: str) -> dict[str, ConsumerInfo]:
        """
        Return info of consumers
        """
        consumers = self.redis_server.xinfo_consumers(
            name=channel, groupname=self.module_name)
        return {_c["name"].decode(): ConsumerInfo(**_c) for _c in consumers}

    def __elect_master(self, info: dict[str, ConsumerInfo]) -> str:
        """
        Using info of consumers to elect master
        """
        master, current = self.module_uid, info[self.module_uid].pending
        if current == 0:
            return master
        for _name, _info in info.items():
            if _info.pending == 0:
                return _name
            elif _info.pending < current:
                master, current = _name, _info.pending
        return master

    def __delete_inactive_consumers(
        self,
        consumers_info: dict[str, ConsumerInfo],
        dead_time: int = 60000
    ) -> dict[str, ConsumerInfo]:
        _to_delete = set()
        for _consumer, _info in consumers_info.items():
            if _info.idle > dead_time:
                self.redis_server.xgroup_delconsumer(
                    name=self.receive_channel.stream_name,
                    groupname=self.module_name,
                    consumername=_consumer
                )
                _to_delete.add(_consumer)
        for _t in _to_delete:
            del consumers_info[_t]

    def message_autoclaim_to_master(self, master: str, idle_time: int = 60000):
        """
        Auto-claim messages to master if idle time surpassed
        """
        res = self.redis_server.xautoclaim(
            name=self.channel["recieving"], groupname=self.module_name, consumername=master, min_idle_time=idle_time)
        # print(res)

    def add_token(self, _cmd_pkg: CommandPackage):
        _cmd_pkg.token = Encryption.build_token()
        self.owned_token.add(_cmd_pkg.token)

    def send_message(self, command: str, sending_channel: str = "", **kwargs) -> None:
        '''
            Format the command package and send the message to the specified channel
        '''
        _package = CommandPackage(type="SHOOT", command=command, **kwargs)
        if sending_channel == self.receive_channel.stream_name:
            self.receive_channel.add_data(_package.model_dump())
        else:
            self.send_channel.add_data(_package.model_dump())
        return None

    def send_command(self, command: str, sending_channel: str = "", **kwargs) -> None:
        '''
            Format the command package and send the command to the specified channel
        '''
        _package = CommandPackage(type="CONFIRM", command=command, **kwargs)
        self.add_token(_package)
        if sending_channel == self.receive_channel.stream_name:
            self.receive_channel.add_data(_package.model_dump())
        else:
            self.send_channel.add_data(_package.model_dump())

    def send_callback(self, command: str, sending_channel: str = "", **kwargs) -> dict:
        '''
            Format the command package and send the command to the specified channel
        '''
        _package = CommandPackage(type="CALLBACK", command=command, **kwargs)
        self.add_token(_package)

        self.send_channel.add_data(_package.model_dump())
        return _package.token

    def wait_for_callback(self, token_list: list[str]):
        count = 0
        _result = [dict()] * len(token_list)
        while count < len(token_list):
            for index, _token in enumerate(token_list):
                if self.command_data.get(_token):
                    _result[index] = self.command_data.pop(_token)
                    count += 1
        return _result

    def message_confirm_and_ack_delete(self, _msg: str):
        '''
            Confirm the message and delete it from the stream
        '''
        self.redis_server.xack(self.receiving_channel, self.module_name, _msg)
        self.redis_server.xdel(self.receiving_channel, _msg)
        self.resolved += 1
        print(
            f"\033[3A\033[1G\033[2KDataQueue: {self.owned_data}\n"
            f"\033[1G\033[2KProcessed Data: {self.processed}\n"
            f"\033[1G\033[2KResolved Number: {self.resolved}\n"
            f"\033[1G\033[2KListened Data: {self.listened}",
            end="",
            flush=True
        )

    def channel_subscribe(self, topics) -> PubSub:
        '''
            Subscribe to the specified channel
        '''
        _redis_instance = self.redis_server.pubsub()
        _redis_instance.subscribe(topics)
        return _redis_instance

    def put_message_to_working_thread(self, data: dict) -> None:
        '''
            Put the message to the working thread
        '''
        try:
            self.data_streaming.put(data)
            self.owned_data = self.data_streaming.qsize()
            print(
                f"\033[3A\033[1G\033[2KDataQueue: {self.owned_data}\n"
                f"\033[1G\033[2KProcessed Data: {self.processed}\n"
                f"\033[1G\033[2KResolved Number: {self.resolved}\n"
                f"\033[1G\033[2KListened Data: {self.listened}",
                end="",
                flush=True
            )
        except TypeError:
            traceback.print_exc()

    def read_data_in_stream(self, stream_id: str):
        '''
            Read the data in the stream
        '''
        data = self.redis_server.xrange(
            self.receive_channel.stream_name, min=stream_id, max=stream_id)
        return redis_xrange_to_python(data)

    def __register_class_method(self, _class):
        return {attribute: getattr(_class, attribute) for attribute in dir(_class) if callable(getattr(_class, attribute)) and attribute.startswith('__') is False and attribute.startswith('_') is False}

    def queuing_data_processing(self, data_queue: queue.Queue):
        '''
            Process the data in the queue
        '''
        while True:
            _queuing_data: dict = data_queue.get()
            _data = CommandPackage(**_queuing_data)
            self.owned_data = data_queue.qsize()
            if _data is not None:
                # Decide whether data need to be executed(Store in self.command)
                try:
                    if _data.command not in self.command_function:
                        raise StreamingException(
                            "Command function not registerd")
                    result = self.command_function[_data.command](
                        json.loads(_data.data))
                    # self.command_data[_data.token] = _data.data
                    self.processed += 1
                    print(
                        f"\033[3A\033[1G\033[2KDataQueue: {self.owned_data}\n"
                        f"\033[1G\033[2KProcessed Data: {self.processed}\n"
                        f"\033[1G\033[2KResolved Number: {self.resolved}\n"
                        f"\033[1G\033[2KListened Data: {self.listened}",
                        end="",
                        flush=True
                    )
                    if _data.type == "SHOOT":
                        self.receive_channel.ack_data(
                            group_name=self.module_name, ids={_data.entry_id})
                        self.receive_channel.del_data(ids={_data.entry_id})
                    elif _data.type == "CONFIRM" or _data.type == "CALLBACK":
                        _data.response = result
                        self.redis_server.publish(
                            self.subscribe_topics, json.dumps(_data.model_dump()))
                except Exception as e:
                    print(e)
            data_queue.task_done()

    def pubsub_listening(self, pubsub: PubSub):
        '''
            Listen to the broadcast channel
        '''
        for i in pubsub.listen():
            if i["type"] == "message":
                self.listened += 1
                print(
                    f"\033[3A\033[1G\033[2KDataQueue: {self.owned_data}\n"
                    f"\033[1G\033[2KProcessed Data: {self.processed}\n"
                    f"\033[1G\033[2KResolved Number: {self.resolved}\n"
                    f"\033[1G\033[2KListened Data: {self.listened}",
                    end="",
                    flush=True
                )
                data = redis_subscribe_to_python(i)
                _package = CommandPackage(**data)
                if _package.token in self.owned_token:
                    if _package.type == "CALLBACK":
                        self.command_data[_package.token] = _package.response
                    self.send_channel.ack_data(
                        group_name=self.module_name, ids={_package.entry_id})
                    self.send_channel.del_data(ids={_package.entry_id})
                    # self.message_confirm_and_ack_delete(_msg=data.get("id"))

    def pending_list_processing(self):
        '''
            Process the pending list
        '''
        while True:
            info = self.__look_up_consumers(
                channel=self.receive_channel.stream_name)
            self.__delete_inactive_consumers(consumers_info=info)
            # Choose master which has the minimum pending messages
            # print(info)
            master_id = self.__elect_master(info)
            # Process self's pending list
            pending_list = self.redis_server.xpending_range(
                name=self.receive_channel.stream_name,
                groupname=self.module_name,
                consumername=self.module_uid,
                min="-",
                max="+",
                count=1000)
            self.redis_server.xpending(
                name=self.receive_channel.stream_name, groupname=self.module_name)
            # print(f"PEL:{len(pending_list)} myself={self.workerID} master={master_id}")
            for _p_info in pending_list:
                # Only dealing with messages that are belong to itself and stay there surpass 3 seconds
                if _p_info["time_since_delivered"] > 3000:
                    data = self.read_data_in_stream(
                        _p_info["message_id"].decode())
                    # self.command_function[data["command"]](json.loads(data["data"]))
                    try:
                        for _d in data:
                            _d["entry_id"] = _p_info["message_id"].decode()
                            _d["token"] = Encryption.build_token()
                            self.owned_token.add(_d["token"])
                            # self.command_function[_d["command"]](json.loads(_d["data"]))
                            self.put_message_to_working_thread(_d)
                            # self.exec_cmd(cmd_package_list=data)
                            # print("data_received_from_queue")
                    except Exception:
                        pass

            # claim other messages if idle time is greater than 5 seconds and not belong to itself
            self.message_autoclaim_to_master(master=master_id)
            # time.sleep(5)

    def stream_listening(self, consumer_group: str, consumer: str, count: int, block: bool = False):
        '''
            Listen to the stream
        '''
        while True:
            data = self.receive_channel.read_group_data(
                group_name=consumer_group,
                consumer_name=consumer,
                count=count,
                block=block)
            if data:
                result = redis_xread_to_python(data)
                for _d in result:
                    self.put_message_to_working_thread(_d)
            self.data_streaming.join()

    def __build_stream_thread(self, thread_name: str, consumer_group: str, consumer: str, count: int, block: bool = False):
        # Once get the message, back to the starting point to read message
        self.main_thread = RestartableThread(target=self.stream_listening,
                                             args=(consumer_group,
                                                   consumer, count, block),
                                             name=thread_name)
        self.main_thread.start()

    def __build_working_thread(self, thread_name: str, streaming_data: queue.Queue):
        self.working_thread = RestartableThread(target=self.queuing_data_processing,
                                                args=(streaming_data, ),
                                                name=thread_name)
        self.working_thread.start()

    def __build_pubsub_thread(self, thread_name: str):
        _instance = self.channel_subscribe(self.subscribe_topics)
        self.subpub_thread = RestartableThread(target=self.pubsub_listening,
                                               args=(_instance, ),
                                               name=thread_name)
        self.subpub_thread.start()

    def __build_pending_thread(self, thread_name: str):
        self.pending_thread = RestartableThread(target=self.pending_list_processing,
                                                name=thread_name)
        self.pending_thread.start()


if __name__ == "__main__":
    from utils.test_function import TestClass
    from module import Streaming
    TestClass()
    cb = Streaming(
        user_module="sammy",
        redis_host="127.0.0.1",
        redis_port=6379,
        redis_db=13,
        receiving_channel_pair=("receiving", {"sammy"}),
        sending_channel_pair=("sending", {"ted"})
    )
