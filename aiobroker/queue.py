import time
from contextlib import contextmanager
from asyncio.queues import Queue, QueueFull


class QueueBusy(Exception):
    pass


class DynamicBoundedManualAcknowledgeQueue(Queue):
    def __init__(self, maxsize=0, flow_control=True, loop=None):
        super().__init__(maxsize, loop=loop)
        self._flow_control = flow_control
        self.ack_t = time.time()
        self.ack_c = 0
        self.nack_c = 0
        self.put_c = 0
        self.drop_p = 0

    def busy(self):
        return 0 < self._maxsize <= self._unfinished_tasks

    def put_nowait(self, item):
        if self.full():
            raise QueueFull
        if self._flow_control:
            self.put_c += 1
            if self.put_c % 100 < self.drop_p:
                self.ack_c += 1  # чтобы правильно считался self.drop_p
                raise QueueBusy
        self._put(item)
        self._unfinished_tasks += 1
        self._wakeup_next(self._getters)

    def ack(self):
        if self._flow_control:
            self.ack_c += 1
            if time.time() - self.ack_t >= 10:
                if self.put_c > 0:

                    # на основе предыдущего опыта (процент успешно обработанных
                    # сообщений) уменьшаем или увеличиваем процент дропа
                    # сообщений коэффициентом drop_p, чтобы ограничить
                    # рост очереди

                    # на основе предыдущего опыта (процент неуспешно
                    # обработанных сообщений) уменьшаем или увеличиваем процент
                    # дропа сообщений коэффициентом e_k, таким образом,
                    # предугадывая результат обработки следующего сообщения
                    # и не пытаясь обработать, если не сможем

                    # e_k не должен быть нулевым, чтобы дать возможность
                    # обработать больше сообщений при увеличении
                    # производительности обработчика

                    e = self.nack_c / (self.ack_c + self.nack_c)
                    if e < 0.1:
                        e_k = 0.5
                    elif e < 0.25:
                        e_k = 0.8
                    elif e < 0.5:
                        e_k = 0.9
                    else:
                        e_k = 1
                    self.drop_p = int(100 * (1 - self.ack_c / self.put_c) * e_k)
                    if self.drop_p < 0:
                        self.drop_p = 0
                else:
                    self.drop_p = 0
                self.ack_t = time.time()
                self.ack_c = 0
                self.nack_c = 0
                self.put_c = 0
        self.task_done()

    def nack(self):
        if self._flow_control:
            self.nack_c += 1
        self.task_done()

    @contextmanager
    def acknowledge(self):
        try:
            yield
        finally:
            self.ack()

    def task_done(self):
        if self._unfinished_tasks <= 0:
            raise ValueError('task_done() called too many times')
        self._unfinished_tasks -= 1

    async def join(self):
        raise NotImplementedError
