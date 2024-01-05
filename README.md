1. First version of "Streaming". Write it in module.py
2. Using multithread.py to handle multi-thread situation. 
3. MainThread as a host which represents module. Module utilized Streaming to communicate with other modules.
4. There are two threads in the back. One to act as a consumer to consume redis stream. The other listen to channel to make sure cammand has its callback. 