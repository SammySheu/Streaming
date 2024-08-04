import time
from module import Streaming


class TestClass:

    @staticmethod
    @Streaming.cmd_register
    def exec_time_cmd(data) -> bool:
        """
        This function executes a command with a specified time sleep.
        """
        if data.get("timesleep"):
            time.sleep(data.get("timesleep"))
        return f'Sleep for {data.get("timesleep")}'

    @staticmethod
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

    @staticmethod
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
