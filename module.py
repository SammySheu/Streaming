"""
This module provides functionality for streaming data.
"""

import os
import time
import json
import queue
import sys
from typing import Callable

import redis
from redis.client import PubSub

from schemas.schemas import (
    CommandPackage, ConsumerInfo, GroupInfo, StreamInfo, GroupData, SubscribeData, PendingInfo, StreamData, PendingData)
from utils.thread_manager import ThreadManager
from utils.encryption import Encryption
from utils.exception import StreamingException
from utils.stream_operate import StreamOperate


class Streaming():
    '''
    user_module: str - specify which module is using.
        It is used to create a consumer group.
    block: int - specify the block time for reading the stream,
        processing from queue.Queue, and pubsub listening timeout.
    debug: bool - specify whether to print the verbose information.
    '''
    command_function = {}

    def __init__(self,
                 user_module: str,
                 redis_host: str,
                 redis_port: int,
                 redis_db: int,
                 block: int = 0,  # in seconds
                 debug: bool = False,
                 ) -> None:
        self.module_name = user_module
        self.module_uid = f"{user_module}_{os.getpid()}"
        self.channel = f"{user_module}Server"
        self.stream_operator = StreamOperate(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            stream_name=self.channel,
            consumer_group=user_module
        )
        self.redis_server = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db)
        self.callback_queue = queue.Queue()  # [(<token>, <data>)]
        self.stream_queue = queue.Queue()
        self.owned_token = set()
        self.subscribe_topics: str = "ResolvedChannel"
        self.listened = 0
        self.resolved = 0
        self.processed = 0
        self.queued_data = 0
        self.debug = debug
        self.__verbose()
        self.thread_manager = ThreadManager()
        self.__build_stream_thread(thread_name="StreamListening",
                                   consumer_group=self.module_name,
                                   consumer=self.module_uid,
                                   count=1,
                                   block=block,
                                   daemon=block != 0)
        self.__build_working_thread(
            thread_name="QueuingDataProcessing", daemon=block != 0)
        self.__build_pubsub_thread(
            thread_name="BroadcastListening", daemon=block != 0)
        self.__build_pending_thread(thread_name="PendingListProcessing")
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

    def __look_up_stream(self) -> StreamInfo:
        return StreamInfo(**self.stream_operator.stream_info())

    def __look_up_group(self) -> dict[str, GroupInfo]:
        return {_g["name"].decode(): GroupInfo(**_g) for _g in self.stream_operator.group_info()}

    def __look_up_consumers(self) -> dict[str, ConsumerInfo]:
        return {
            _c["name"].decode(): ConsumerInfo(**_c)
            for _c in self.stream_operator.consumer_info(group_name=self.module_name)
        }

    def __look_up_pending(self, group: str) -> PendingInfo:
        _pending_info = self.stream_operator.pending_info(group_name=group)
        _pending_info["consumers"] = self.stream_operator.consumer_info(
            group_name=group)
        return PendingInfo(**_pending_info)

    def __elect_master(self, idle_time_less_than: int = sys.maxsize) -> str:
        """
        Elect master if idle time is less than the specified time.
        """
        consumers = self.__look_up_consumers()
        if consumers[self.module_uid].pending == 0 and consumers[self.module_uid].idle < idle_time_less_than:
            return self.module_uid
        master, cur = self.module_uid, sys.maxsize
        for _consumer_info in consumers.values():
            if _consumer_info.idle < idle_time_less_than:
                if _consumer_info.pending == 0:
                    return _consumer_info.name
                elif _consumer_info.pending < cur:
                    master, cur = _consumer_info.name, _consumer_info.pending
        return master

    def __elect_master_and_claim_data(self, data: list[PendingData], min_idle_time: int = sys.maxsize):
        _master = self.__elect_master(idle_time_less_than=min_idle_time)
        self.__claim_pending_data(
            pending_data=data, consumer_name=_master, min_idle_time=min_idle_time)

    def __claim_pending_data(self, pending_data: list[PendingData], consumer_name: str, min_idle_time: int):
        self.stream_operator.claim_data(self.module_name, consumer_name=consumer_name,
                                        min_idle_time=min_idle_time, ids=[
                                            _p.message_id for _p in pending_data])

    def batch_claim_data_to_master(self, batch: int, min_idle_time: int) -> set[str]:
        pending_data = [PendingData(**_data) for _data in self.stream_operator.pending_range(
            group_name=self.module_name, count=10000, idle=min_idle_time)]
        if pending_data:
            i = 0
            while i+batch < len(pending_data):
                self.__elect_master_and_claim_data(
                    data=pending_data[i:i+batch], min_idle_time=min_idle_time)
                i += batch
            self.__elect_master_and_claim_data(
                data=pending_data[i:], min_idle_time=min_idle_time)

    def delete_inactive_consumers(
        self,
        dead_time: int
    ) -> dict[str, ConsumerInfo]:
        del_consumers = set()
        for _name, _info in self.__look_up_consumers().items():
            if _info.idle > dead_time:
                del_consumers.add(_name)
        if del_consumers:
            # Only batch claim pending data if there are inactive consumers
            self.batch_claim_data_to_master(batch=5, min_idle_time=dead_time)
            for _consumer in del_consumers:
                self.stream_operator.delete_consumer(
                    group_name=self.module_name, consumer_name=_consumer)

    def message_autoclaim_to_master(self, master: str, min_idle_time: int = 10000, count: int = 1):
        """
        Auto-claim messages to master if idle time surpassed
        """
        self.stream_operator.autoclaim_data(
            group_name=self.module_name, consumer_name=master, min_idle_time=min_idle_time, count=count)

    def add_token(self, _cmd_pkg: CommandPackage):
        _cmd_pkg.token = Encryption.build_token()
        self.owned_token.add(_cmd_pkg.token)

    def send_message(self, command: str, sending_channel: str = "", **kwargs) -> None:
        '''
        Format the command package and send the message to the specified channel
        '''
        _package = CommandPackage(type="SHOOT", command=command, **kwargs)
        _sending_channel = sending_channel if sending_channel else self.channel
        self.stream_operator.add_data(
            stream_name=_sending_channel, data=_package.model_dump())
        return None

    def send_command(self, command: str, sending_channel: str = "", **kwargs) -> None:
        '''
        Format the command package and send the command to the specified channel
        '''
        _package = CommandPackage(type="CONFIRM", command=command, **kwargs)
        self.add_token(_package)
        _sending_channel = sending_channel if sending_channel else self.channel
        self.stream_operator.add_data(
            stream_name=_sending_channel, data=_package.model_dump())

    def send_callback(self, command: str, sending_channel: str, **kwargs) -> dict:
        '''
        Format the command package and send the command to the specified channel
        '''
        _sending_channel = sending_channel if sending_channel else self.channel
        _package = CommandPackage(
            type="CALLBACK", command=command, channel_name=_sending_channel, **kwargs)
        self.add_token(_package)
        self.stream_operator.add_data(
            stream_name=_sending_channel, data=_package.model_dump())
        return _package.token

    def wait_for_callback(self, *token_list: tuple[str]) -> list:
        _require = len(token_list)
        _return = [""] * _require
        count = 0
        while count < _require:
            token, data = self.callback_queue.get()
            if token in token_list:
                _return[token_list.index(token)] = data
                count += 1
            else:
                raise StreamingException("Token not found")
        return _return

    def __verbose(self):
        if self.debug:
            print(
                f"\033[3A\033[1G\033[2KDataQueue: {self.queued_data}\n"
                f"\033[1G\033[2KProcessed Data: {self.processed}\n"
                f"\033[1G\033[2KResolved Number: {self.resolved}\n"
                f"\033[1G\033[2KListened Data: {self.listened}",
                end="",
                flush=True
            )

    def broadcast_message(self, command: str, **kwargs) -> None:
        '''
        Broadcast the message to all the user_module
        '''
        _package = CommandPackage(type="BROADCAST", command=command, **kwargs)
        self.redis_server.publish(
            self.subscribe_topics, json.dumps(_package.model_dump()))
        return None

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
            self.stream_queue.put(data)
            self.queued_data = self.stream_queue.qsize()
            self.__verbose()
        except TypeError as e:
            print(e)
            # traceback.print_exc()

    def _convert_group_data(self, data) -> list[CommandPackage]:
        '''
        Convert the group data to CommandPackage
        '''
        _result = []
        group_data = GroupData(data[0])
        for _stream_data in group_data.stream_datas:
            _stream_data.values["entry_id"] = _stream_data.key
            _result.append(CommandPackage(**_stream_data.values))
        return _result

    def _convert_range_data(self, data) -> list[CommandPackage]:
        _result = []
        stream_data = [StreamData(_d) for _d in data]
        for _stream_data in stream_data:
            _stream_data.values["entry_id"] = _stream_data.key
            _result.append(CommandPackage(**_stream_data.values))
        return _result

    def _convert_subscribe_data(self, data) -> SubscribeData:
        '''
        Convert the subscribe data to CommandPackage
        '''
        return SubscribeData(**data)

    def __register_class_method(self, _class):
        return {attribute: getattr(_class, attribute) for attribute in dir(_class) if callable(getattr(_class, attribute)) and attribute.startswith('__') is False and attribute.startswith('_') is False}

    def queuing_data_processing(self, stop_event):
        '''
        Process the data in the queue
        '''
        while not stop_event.is_set():
            try:
                _queuing_data: dict = self.stream_queue.get(timeout=5)
            except queue.Empty:
                continue
            _data = CommandPackage(**_queuing_data)
            self.queued_data = self.stream_queue.qsize()
            if _data is not None:
                # Decide whether data need to be executed(Store in self.command)
                try:
                    if _data.command not in self.command_function:
                        raise StreamingException(
                            "Command function not registerd")
                    result = self.command_function[_data.command](
                        _data.data)
                    if _data.type == "SHOOT":
                        # pass
                        self.ack_and_delete_message(
                            stream_name=_data.channel_name,
                            group_name=self.module_name,
                            ids={_data.entry_id})
                    elif _data.type == "CONFIRM" or _data.type == "CALLBACK":
                        _data.response = result
                        self.redis_server.publish(
                            self.subscribe_topics, json.dumps(_data.model_dump()))
                    self.processed += 1
                    self.__verbose()
                except Exception as e:
                    pass
                    # raise StreamingException(str(e)) from e
            self.stream_queue.task_done()

    def pubsub_listening(self, stop_event, pubsub: PubSub):
        '''
        Listen to the broadcast channel
        '''
        try:
            while not stop_event.is_set():
                i = pubsub.get_message(timeout=5)
                if i is None:
                    continue
                _subscribe = self._convert_subscribe_data(i)
                if _subscribe.type == "message":
                    self.listened += 1
                    self.__verbose()
                    # Only dealing with messages that are belong to itself
                    if _subscribe.data.token in self.owned_token:
                        if _subscribe.data.type == "CALLBACK":
                            # Only put listened data into callback_queue if it is type of callback
                            self.callback_queue.put(
                                (_subscribe.data.token, _subscribe.data.response))
                        self.ack_and_delete_message(
                            stream_name=_subscribe.data.channel_name, group_name=self.module_name, ids={_subscribe.data.entry_id})
                        self.resolved += 1
                        self.__verbose()
                    else:
                        if _subscribe.data.type == "BROADCAST":
                            self.put_message_to_working_thread(
                                _subscribe.data.model_dump())
        except Exception as e:
            print(e)

    def ack_and_delete_message(self, stream_name: str, group_name: str, ids: set[str]):
        '''
            Acknowledge and delete the message in the stream
        '''
        self.stream_operator.ack_data(
            stream_name=stream_name, group_name=group_name, ids=ids)
        self.stream_operator.del_data(
            stream_name=stream_name, ids=ids)

    def pending_list_processing(self, _stop_event):
        '''
        Process the pending list
        '''
        while not _stop_event.is_set():

            # Process self's pending list
            pending_list = self.stream_operator.pending_range(
                group_name=self.module_name, count=1000, consumer_name=self.module_uid, idle=5000)
            _range_data = []
            for _p_info in pending_list:
                _range_data.extend(self.stream_operator.read_data_by_id(
                    _id=_p_info["message_id"].decode()))
            if _range_data:
                result: list[CommandPackage] = self._convert_range_data(
                    _range_data)
                for _cmd_pkg in result:
                    self.put_message_to_working_thread(_cmd_pkg.model_dump())
                    self.stream_queue.join()

            # delete inactive consumers and claim pending data to the other worker
            self.delete_inactive_consumers(dead_time=60000)
            time.sleep(5)

    def stream_listening(self, stop_event, consumer_group: str, consumer: str, count: int, block: bool = False):
        '''
        Listen to the stream
        '''
        while not stop_event.is_set():
            data = self.stream_operator.read_group_data(
                group_name=consumer_group,
                consumer_name=consumer,
                count=count,
                block=block)
            if data:
                result: list[CommandPackage] = self._convert_group_data(data)
                for _cmd_pkg in result:
                    self.put_message_to_working_thread(_cmd_pkg.model_dump())
            self.stream_queue.join()

    def __build_stream_thread(
        self,
        thread_name: str,
        consumer_group: str,
        consumer: str,
        count: int,
        block: int,
        daemon: bool
    ):
        """
        Builds a stream thread for listening to messages.
        """
        # Once get the message, back to the starting point to read message
        self.thread_manager.add_thread(target_func=self.stream_listening, args=(
            consumer_group, consumer, count, block*1000), name=thread_name, daemon=daemon)

    def __build_working_thread(self, thread_name: str, daemon: bool):
        """
        Builds a working thread for processing(executing) messages.
        """
        self.thread_manager.add_thread(target_func=self.queuing_data_processing,
                                       name=thread_name,
                                       daemon=daemon)

    def __build_pubsub_thread(self, thread_name: str, daemon: bool):
        '''
        Builds a pubsub thread for listening to the broadcast channel
        '''
        _instance = self.channel_subscribe(self.subscribe_topics)
        self.thread_manager.add_thread(target_func=self.pubsub_listening,
                                       args=(_instance, ),
                                       name=thread_name,
                                       daemon=daemon)

    def __build_pending_thread(self, thread_name: str):
        self.thread_manager.add_thread(target_func=self.pending_list_processing,
                                       name=thread_name)

    def exit(self):
        self.thread_manager.exit()

    def __del__(self):
        self.exit()


@ Streaming.cmd_register
def Information(tmp):
    return f"Information: {tmp}"


@ Streaming.cmd_register
def CPU_intensive_task(data: dict):
    start = time.time()

    def fibonacci(n):
        if n <= 1:
            return n
        else:
            return fibonacci(n-1) + fibonacci(n-2)
    result = fibonacci(data.get("fibonacci_number"))
    return time.time() - start


if __name__ == "__main__":
    cb = Streaming(user_module="Sammy", redis_host="localhost",
                   redis_port=6379, redis_db=0, block=5, debug=True)
    [cb.send_command(sending_channel="SammyServer", command="Information",
                     data=json.dumps({"msg": "Hello"})) for _ in range(100)]
    while True:
        time.sleep(3600)
