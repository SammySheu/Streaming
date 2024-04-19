import time

def IO_intensive_task(data: dict):
    with open(data.get("input_file"), 'r') as file:
        contents = file.readlines()  # 讀取文件全部內容
        with open(data.get("output_file"), 'w') as file:
            for row in contents:
                file.writelines(f"{row[:-2]} World\n")  # 將讀取的數據寫入新文件

def CPU_intensive_task(data: dict):
    start = time.time()
    def fibonacci(n):
        if n <= 1:
            return n
        else:
            return fibonacci(n-1) + fibonacci(n-2)
    result = fibonacci(data.get("fibonacci_number"))
    return time.time() - start

def do_job(queue, id):
    while True:
        data: dict = queue.get()
        if data.get("fibonacci_number") is None:
            return None
        result = CPU_intensive_task(data)
        print(f'pid{id}|{data.get("task_number")} | {result}', flush=True)

def parallel_compute(method: str, total_task: int, parallel_number: int):

    if method == "Thread":
        from threading import Thread as Context
        from queue import Queue
    else:
        from multiprocessing import Process as Context
        from multiprocessing import Queue
    queue = Queue()
    processes = [
        Context(target=do_job, args=(queue, _))
        for _ in range(parallel_number)
    ]   

    for p in processes:
        p.start()
    start = time.time()
    for i in range(total_task):
        queue.put({
            "fibonacci_number": 33,
            "task_number": i})  # 放入資料
    for _ in range(parallel_number*2):
        queue.put({
            "fibonacci_number": None,
            "task_number": i})  # 放入資料
    for p in processes:
        p.join()  # 等待 process 執行結束
    print(time.time() - start)


if __name__ == '__main__':
    # parallel_compute(method="Process", total_task=80, parallel_number=8)
    parallel_compute(method="Process", total_task=80, parallel_number=8)