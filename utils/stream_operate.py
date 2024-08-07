import redis
from redis.exceptions import ResponseError


class StreamOperate:
    def __init__(self, redis_host, redis_port, redis_db, stream_name, groups: set):
        self.redis = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db)
        self.stream_name = stream_name
        for _g in groups:
            try:
                self.create_group(_g)
            except ResponseError as e:
                if "BUSYGROUP Consumer Group name already exists" not in str(e):
                    raise e

    def stream_info(self):
        return self.redis.xinfo_stream(name=self.stream_name)

    def group_info(self):
        return self.redis.xinfo_groups(name=self.stream_name)

    def consumer_info(self, group_name: str):
        return self.redis.xinfo_consumers(name=self.stream_name, groupname=group_name)

    def create_group(self, group_name: str):
        return self.redis.xgroup_create(name=self.stream_name, groupname=group_name, mkstream=True)

    def delete_consumer(self, group_name: str, consumer_name: str):
        return self.redis.xgroup_delconsumer(
            name=self.stream_name, groupname=group_name, consumername=consumer_name)

    def read_group_data(self, group_name: str, consumer_name: str, count: int, block: bool = False):
        return self.redis.xreadgroup(
            groupname=group_name, consumername=consumer_name,
            streams={self.stream_name: ">"}, count=count, block=0 if block else None)

    def add_data(self, data: dict):
        return self.redis.xadd(name=self.stream_name, fields=data)

    def ack_data(self, group_name: str, ids: set):
        return self.redis.xack(self.stream_name, group_name, *ids)

    def del_data(self, ids: set):
        return self.redis.xdel(self.stream_name, *ids)

    def autoclaim_data(self, group_name: str, consumer_name: str, idle_time: int):
        return self.redis.xautoclaim(
            name=self.stream_name, groupname=group_name,
            consumername=consumer_name, min_idle_time=idle_time)
