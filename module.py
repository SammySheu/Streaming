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
from pydantic import ValidationError

from multithread import RestartableThread
from schemas import CommandPackage, ConsumerInfo, GroupInfo, StreamInfo
from encryption import Encryption
from exception import StreamingException


def redis_xread_to_python(data) -> list:
    '''
    Customized function to convert redis xread data to python dictionary
    '''
    try:
        _, _dl = data[0]
        _result = []
        for _d in _dl:
            item_id, td = _d
            _temp = {k.decode(): v.decode() for k, v in td.items()}
            _temp["id"] = item_id.decode()
            _result.append(_temp)
    except ValueError:
        print(ValueError)
    except KeyError:
        print(KeyError)
    return _result


def redis_subscribe_to_python(data):
    '''
    Customized function to convert redis subscribe data to python dictionary
    '''
    data = json.loads(data["data"].decode())
    return data


def redis_xrange_to_python(data):
    '''
    Customized function to convert redis xrange data to python dictionary
    '''
    try:
        _, _dl = data[0]
        _temp = {k.decode(): v.decode() for k, v in _dl.items()}
    except ValueError:
        print(ValueError)
    except KeyError:
        print(KeyError)
    return [_temp]


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
                 receiving_channel: str,
                 sending_channel: str,
                 other_groups: set,
                 asynchronous: bool = False
                 ) -> None:
        self.group = user_module
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.channel = {
            "receiving": receiving_channel,
            "sending": sending_channel
        }
        self.asynchronous = asynchronous
        self.count = 0
        self.callback_total = 0
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
        self.worker_id = f"{self.group}_{os.getpid()}"
        self.time = int(time.time())
        print(
            f"\nDataQueue: {self.owned_data}\n"
            f"\033[1G\033[2KProcessed Data: {self.processed}\n"
            f"\033[1G\033[2KResolved Number: {self.resolved}\n"
            f"\033[1G\033[2KListened Data: {self.listened}",
            end="",
            flush=True
        )

        Streaming.command_function.update(self.__register_class_method(self))
        self.__init_redis()
        self.__create_channel_and_group(
            pair={f"{self.channel['receiving']}": {self.group},
                  f"{self.channel['sending']}": other_groups})
        self.__build_stream_thread(
            consumer_group=self.group, consumer=self.worker_id, block=True)
        self.__build_working_thread()
        self.__build_pubsub_thread()
        # self.__build_pending_thread()

    @classmethod
    def cmd_register(cls, func: Callable) -> Callable:
        """
        Registers a command function in the command_function dictionary of the given class.
        """
        cls.command_function[func.__name__] = func
        return func

    def __init_redis(self) -> None:
        self.redis_server = redis.Redis(
            host=self.redis_host, port=self.redis_port, db=self.redis_db)

    def __create_channel_and_group(self, pair: dict[str, set]) -> None:
        for _channel, _groups in pair.items():
            try:
                group_info = self.__look_up_group(_channel)
            except ResponseError:
                self.redis_server.xgroup_create(
                    name=_channel, groupname=_group, mkstream=True)
            for _group in _groups:
                if _group not in group_info:
                    self.redis_server.xgroup_create(
                        name=_channel, groupname=_group, mkstream=True)

    def __look_up_stream(self, channel: str) -> None:
        try:
            _info = self.redis_server.xinfo_stream(name=channel)
            StreamInfo(**_info)
        except ResponseError as e:
            print(e)
            self.redis_server.xgroup_create(
                name=channel,
                groupname=self.group,
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
            name=channel, groupname=self.group)
        return {_c["name"].decode(): ConsumerInfo(**_c) for _c in consumers}

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
                self.redis_server.xgroup_delconsumer(
                    name=self.channel["receiving"], groupname=self.group, consumername=_consumer)
                _to_delete.add(_consumer)
        for _t in _to_delete:
            del consumers_info[_t]

    # When sending message, token is unneeded and message was acked by others

    def add_message(self, _package: CommandPackage):
        self.redis_server.xadd(name=_package.sending_channel,
                               fields=_package.command_basic.model_dump())

    def message_autoclaim_to_master(self, master: str, idle_time: int = 60000):
        """
        Auto-claim messages to master if idle time surpassed
        """
        res = self.redis_server.xautoclaim(
            name=self.channel["recieving"], groupname=self.group, consumername=master, min_idle_time=idle_time)
        # print(res)

    def add_token(self, _cmd_pkg: CommandPackage):
        _cmd_pkg.command_basic.token = Encryption.build_token()
        self.owned_token.add(_cmd_pkg.command_basic.token)

    # To specify which channel, what type of command
    def format_command(self, **cmd_package) -> CommandPackage:
        try:
            _cp = CommandPackage(**cmd_package)
        except ValidationError as e:
            print(e)
            _cp = CommandPackage()
        if not _cp.sending_channel:
            _cp.sending_channel = self.channel["sending"]
        if _cp.command_basic.command_type == "send_message":
            _cp.command_basic.command_type = "SHOOT"
        elif _cp.command_basic.command_type == "send_command":
            _cp.command_basic.command_type = "CONFIRM"
        if _cp.command_basic.command_type == "send_callback":
            _cp.command_basic.command_type = "CALLBACK"
        return _cp

    def send_message(self, sending_channel: str = "", command: str = "", **kwargs) -> None:
        '''
            Format the command package and send the message to the specified channel
        '''
        _package = self.format_command(sending_channel=sending_channel,
                                       command=command, command_type=self.send_message.__name__,
                                       **kwargs)
        self.add_message(_package)
        return None

    def send_command(self, command: str, sending_channel: str = "", **kwargs) -> None:
        '''
            Format the command package and send the command to the specified channel
        '''
        __package = self.format_command(sending_channel=sending_channel,
                                        command=command, command_type=self.send_command.__name__,
                                        **kwargs)
        self.add_token(__package)
        self.add_message(__package)

    def send_callback(self, command: str, sending_channel: str = "", **kwargs) -> dict:
        '''
            Format the command package and send the command to the specified channel
        '''
        __package = self.format_command(sending_channel=sending_channel,
                                        command=command, command_type=self.send_callback.__name__,
                                        **kwargs)
        self.add_token(__package)

        def _sub_task(cmd_pkg: CommandPackage):
            self.add_message(cmd_pkg)
            self.callback_total += 1
            while True:
                if self.command_data.get(cmd_pkg.command_basic.token):
                    return self.command_data.pop(cmd_pkg.command_basic.token)
        sub_task_result = RestartableThread(target=_sub_task, args=(
            __package, ), name=f"WaitForCallback_{self.callback_total}")
        sub_task_result.start()
        sub_task_result.join()
        return sub_task_result.result

    def message_confirm_and_ack_delete(self, _msg: str):
        '''
            Confirm the message and delete it from the stream
        '''
        self.redis_server.xack(self.receiving_channel, self.group, _msg)
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
            self.channel["receiving"], min=stream_id, max=stream_id)
        return redis_xrange_to_python(data)

    def __register_class_method(self, _class):
        return {attribute: getattr(_class, attribute) for attribute in dir(_class) if callable(getattr(_class, attribute)) and attribute.startswith('__') is False and attribute.startswith('_') is False}

    def queuing_data_processing(self, data_queue: queue.Queue):
        '''
            Process the data in the queue
        '''
        while True:
            _data: dict = data_queue.get()
            self.owned_data = data_queue.qsize()
            if _data is not None:
                # Decide whether data need to be executed(Store in self.command)
                try:
                    if _data["command"] not in self.command_function:
                        raise StreamingException(
                            "Command function not registerd")
                    result = self.command_function[_data["command"]](
                        json.loads(_data["data"]))
                    self.processed += 1
                    print(
                        f"\033[3A\033[1G\033[2KDataQueue: {self.owned_data}\n"
                        f"\033[1G\033[2KProcessed Data: {self.processed}\n"
                        f"\033[1G\033[2KResolved Number: {self.resolved}\n"
                        f"\033[1G\033[2KListened Data: {self.listened}",
                        end="",
                        flush=True
                    )
                    self.redis_server.publish(
                        self.subscribe_topics, json.dumps(_data))
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
                if data["token"] in self.owned_token:
                    self.message_confirm_and_ack_delete(_msg=data.get("id"))

    def pending_list_processing(self):
        '''
            Process the pending list
        '''
        while True:
            info = self.__look_up_consumers(channel=self.channel["receiving"])
            self.__delete_inactive_consumers(consumers_info=info)
            # Choose master which has the minimum pending messages
            # print(info)
            master_id = self.__elect_master(info)
            # Process self's pending list
            pending_list = self.redis_server.xpending_range(
                name=self.channel["receiving"],
                groupname=self.group,
                consumername=self.workerID,
                min="-",
                max="+",
                count=1000)
            self.redis_server.xpending(
                name=self.channel["receiving"], groupname=self.group)
            # print(f"PEL:{len(pending_list)} myself={self.workerID} master={master_id}")
            for _p_info in pending_list:
                # Only dealing with messages that are belong to itself and stay there surpass 3 seconds
                if _p_info["time_since_delivered"] > 3000:
                    data = self.read_data_in_stream(
                        _p_info["message_id"].decode())
                    # self.command_function[data["command"]](json.loads(data["data"]))
                    try:
                        for _d in data:
                            _d["id"] = _p_info["message_id"].decode()
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

    def stream_listening(self, consumer_group: str, consumer: str, block: bool = False):
        '''
            Listen to the stream
        '''
        while True:
            data = self.redis_server.xreadgroup(
                groupname=consumer_group,
                consumername=consumer,
                streams={
                    self.channel["receiving"]: ">"
                },
                count=1,
                block=0 if block else None)
            if data:
                result = redis_xread_to_python(data)
                for _d in result:
                    self.put_message_to_working_thread(_d)
            self.data_streaming.join()

    def __build_stream_thread(self, consumer_group: str, consumer: str, block: bool = False):
        self.worker_id = consumer
        # Once get the message, back to the starting point to read message
        self.main_thread = RestartableThread(target=self.stream_listening,
                                             args=(consumer_group,
                                                   consumer, block),
                                             name="StreamListening")
        self.main_thread.start()

    def __build_working_thread(self):
        self.working_thread = RestartableThread(target=self.queuing_data_processing,
                                                args=(self.data_streaming, ),
                                                name="QueuingDataProcessing")
        self.working_thread.start()

    def __build_pubsub_thread(self):
        _instance = self.channel_subscribe(self.subscribe_topics)
        self.subpub_thread = RestartableThread(target=self.pubsub_listening,
                                               args=(_instance, ),
                                               name="BroadcastListening")
        self.subpub_thread.start()

    def __build_pending_thread(self):
        self.pending_thread = RestartableThread(target=self.pending_list_processing,
                                                name="PendingListProcessing")
        self.pending_thread.start()


@Streaming.cmd_register
def io_intensive_task(data) -> bool:
    """
    This function performs an I/O intensive task.
    """
    start = time.time()
    with open(data.get("input_file"), 'r', encoding='utf-8') as file:
        contents = file.readlines()  # 讀取文件全部內容
        with open(
                f'./task_folder/{data.get("output_file")}_{time.time()}',
                'w', encoding='utf-8') as file:
            for row in contents:
                file.writelines(f"{row[:-2]} World\n")  # 將讀取的數據寫入新文件
    return time.time() - start


@Streaming.cmd_register
def cpu_intensive_task(data: dict):
    """
    This function consumes lots of CPU work.
    """
    start = time.time()

    def fibonacci(n):
        if n <= 1:
            return n
        else:
            return fibonacci(n-1) + fibonacci(n-2)
    fibonacci(data.get("fibonacci_number"))
    return time.time() - start


@Streaming.cmd_register
def exec_time_cmd(data) -> bool:
    """
    This function executes a command with a specified time sleep.
    """
    if data.get("timesleep"):
        time.sleep(data.get("timesleep"))
    return True


if __name__ == "__main__":
    cb = Streaming(
        user_module="sam", redis_host="127.0.0.1", redis_port=6379,
        redis_db=13, receiving_channel="receiving", sending_channel="sending",
        other_groups={"ted", "annie"})
    for _ in range(100):
        cb.send_message(
            sending_channel="receiving",
            command="exec_time_cmd",
            data=json.dumps({
                    "input_file": "IO_task_input.txt",
                    "output_file": "IO_task_output.txt",
                    "timesleep": 3
            }))
