## Workflow
1. First version of "Streaming". Write it in module.py
2. Using multithread.py to handle multi-thread situation. 
3. Using encryption.py to handle token signing
4. MainThread as a host which represents module. Module utilized Streaming to communicate with other modules.
5. There are two threads in the back. One to act as a consumer to consume redis stream. The other listen to channel to make sure cammand has its callback. 
6. Add third thread to read pending list in stream. Make sure it process message which belong to itself every 3 seconds and xClaim other messages to specific consumer if idle time is greater than 5 seconds

## Current Status
1. 可以發送訊息到stream上並且背景Thread(stream_listening)會持續監聽
2. 監聽到的stream會放入working_thread做處理
3. 目前設定一次抓一筆資料且阻塞（count=1 & queue.join() & queue.task_done()）
    * 好處：任務會平均分散給各個worker
    * 壞處：每拿一筆就佔網路I/O，沒有batch功能
    * 此設定可避免一次拿太多任務且無法消化
4. worker閒置超過60秒會被群組踢掉，其底下的pending_data會平均claim到其他worker
5. 發送訊息: 可選擇四種模式
   * send_message: 單純發送訊息，不做後續處理
   * send_command: 發送命令且對方確認後，做ack&delete
   * send_callback: 發送回呼函數，須經對方傳回response才ack&delete
   * broadcast: 經由pubsub發送一次性 request, 不做後續處理