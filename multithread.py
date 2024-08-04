from threading import Thread
import threading
import inspect
import ctypes


class RestartableThread(Thread):
    def __init__(
        self,
        group=None,
        target=None,
        name=None,
        args=(),
        *,
        daemon=None,
        kwargs: dict = None
    ):
        self.result = None
        # self.target = target
        self.args_option = args
        self.daemon_option = daemon
        self.kwargs_option = kwargs
        Thread.__init__(self, group, target, name=name,
                        args=args, kwargs=kwargs, daemon=daemon)

    def get_id(self):
        # returns id of the respective thread
        if hasattr(self, '_thread_id'):
            return self._thread_id

        for id, thread in threading._active.items():
            if thread is self:
                return id

    # def run(self) -> None:
    #     self.result = self.target(self.args_option)

    def clone(self):
        clone_td = RestartableThread(
            self._group,
            self._target,
            self._name,
            self.args_option,
            daemon=self.daemon_option,
            kwargs=self.kwargs_option
        )
        return clone_td

    def restart(self):
        clone_td = self.clone()
        clone_td.start()
        return clone_td

    def exit(self):
        if self.is_alive():
            thread_id = self.get_id()
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                thread_id, ctypes.py_object(SystemExit))
            if res > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
                print('Exception raise failure')
            stop_thread(self)


def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    tid = ctypes.c_long(tid)
    if not inspect.isclass(exctype):
        exctype = type(exctype)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


def stop_thread(thread):
    _async_raise(thread.ident, SystemExit)
