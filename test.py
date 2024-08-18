import json
from module import Streaming
from utils.test_function import TestClass
TestClass()

cb = Streaming(
    user_module="Test",
    redis_host="127.0.0.1",
    redis_port=6379,
    redis_db=0)
tasks = []
for _ in range(100):
    tasks.append(cb.send_callback(
        sending_channel="TestServer",
        command="exec_time_cmd",
        data=json.dumps({
            "input_file": "IO_task_input.txt",
            "output_file": "IO_task_output.txt",
            "timesleep": 0.1
        })))
answer = cb.wait_for_callback(*tasks)
print(answer)
