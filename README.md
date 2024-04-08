## Workflow
1. First version of "Streaming". Write it in module.py
2. Using multithread.py to handle multi-thread situation. 
3. Using encryption.py to handle token signing
4. MainThread as a host which represents module. Module utilized Streaming to communicate with other modules.
5. There are two threads in the back. One to act as a consumer to consume redis stream. The other listen to channel to make sure cammand has its callback. 
6. Add third thread to read pending list in stream. Make sure it process message which belong to itself every 3 seconds and xClaim other messages to specific consumer if idle time is greater than 5 seconds

## Current Status
1. 可以發送訊息到stream上
2. 每一個reciever上線時，會去抓100筆資料下來處理，直到處理完畢才會繼續抓