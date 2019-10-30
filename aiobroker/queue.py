import asyncio
import logging
import time
from asyncio import Future
from functools import partial
from typing import Dict, Optional, Any, Callable, NoReturn
from uuid import UUID, uuid4
from contextlib import contextmanager, suppress
from asyncio.queues import Queue, QueueFull
from aiobroker.exceptions import ConsumerDoesNotExist


UNLIMITED = 0
RANDOM_NAME = None

ConsumerTag = UUID


class QueueBusy(Exception):
    pass


class DynamicBoundedManualAcknowledgeQueue(Queue):

    log = logging.getLogger('aiobroker.queue')

    def __init__(self,
                 maxsize: int = UNLIMITED,
                 flow_control: bool = True,
                 name: str = RANDOM_NAME,
                 prefetch_count: int = UNLIMITED,  # shared across all consumers
                 auto_delete: bool = True,
                 ttl: int = None,
                 loop: Optional[asyncio.AbstractEventLoop] = None):

        super().__init__(maxsize, loop=loop)
        self.name = name or str(uuid4())
        self.prefetch_count = prefetch_count
        self.auto_delete = auto_delete
        self.ttl = ttl
        self._flow_control = flow_control
        self.ack_t = time.time()
        self.ack_c = 0
        self.nack_c = 0
        self.put_c = 0
        self.drop_p = 0

        self.__consumers: Dict[ConsumerTag, asyncio.Task] = {}
        if self.prefetch_count:
            self.prefetch_semaphore = asyncio.Semaphore(self.prefetch_count)
        if self.ttl:
            self.expiration_task = asyncio.create_task(
                self.__remove_expired_messages())

    def busy(self):
        # noinspection PyUnresolvedReferences
        return 0 < self._maxsize <= self._unfinished_tasks

    def put_nowait(self, item):
        if self.full():
            raise QueueFull
        if self._flow_control:
            self.put_c += 1
            if self.put_c % 100 < self.drop_p:
                self.ack_c += 1  # чтобы правильно считался self.drop_p
                raise QueueBusy
        if self.ttl:
            self._put((item, time.time()))
        else:
            self._put(item)
        # noinspection PyUnresolvedReferences
        self._unfinished_tasks += 1
        # noinspection PyUnresolvedReferences
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
        # noinspection PyUnresolvedReferences
        if self._unfinished_tasks <= 0:
            raise ValueError('task_done() called too many times')
        # noinspection PyUnresolvedReferences
        self._unfinished_tasks -= 1
        if self.prefetch_count:
            self.prefetch_semaphore.release()

    async def join(self):
        raise NotImplementedError

    async def __consume(self,
                        on_message: Callable[[Any], Any],
                        auto_ack: bool = True,
                        on_error: Optional[Callable[[Exception], Any]] = None):

        # TODO: сделать предварительно несколько вариантов этого метода
        #  чтобы в рантайме не делать проверки выполнения условия
        auto_ack_callback = partial(self.__on_message_processed,
                                    on_error=on_error)
        error_callback = partial(self.__on_message_processing_failed,
                                 on_error=on_error)
        with suppress(asyncio.CancelledError):
            while True:
                if self.prefetch_count:
                    await self.prefetch_semaphore.acquire()
                if self.ttl:
                    message, _ = await self.get()
                else:
                    message = await self.get()
                if auto_ack:
                    task = asyncio.create_task(on_message(message))
                    task.add_done_callback(auto_ack_callback)
                else:
                    task = asyncio.create_task(
                        on_message(QueueMessage(message, self)))
                    task.add_done_callback(error_callback)

    def basic_consume(self,
                      on_message: Callable[[Any], Any],
                      auto_ack: bool = True,
                      on_error: Optional[Callable[[Exception], Any]] = None) \
            -> ConsumerTag:

        task = asyncio.create_task(
            self.__consume(on_message, auto_ack, on_error))
        consumer_tag = uuid4()
        self.__consumers[consumer_tag] = task
        return consumer_tag

    def basic_cancel(self, consumer_tag: ConsumerTag):
        try:
            consumer = self.__consumers.pop(consumer_tag)
            consumer.cancel()
        except KeyError:
            raise ConsumerDoesNotExist(consumer_tag)

    def close(self):
        for consumer in self.__consumers.values():
            consumer.cancel()
        if self.ttl:
            self.expiration_task.cancel()

    def __on_message_processed(self, fut, on_error=None):
        # type: (Future, Optional[Callable[[Exception], Any]]) -> NoReturn
        try:
            fut.result()
            self.ack()
        except Exception as e:
            self.nack()
            if on_error:
                on_error(e)

    def __on_message_processing_failed(self, fut, on_error=None):
        # type: (Future, Optional[Callable[[Exception], Any]]) -> NoReturn
        with suppress(asyncio.CancelledError):
            exc = fut.exception()
            if exc:
                self.nack()
                if on_error:
                    on_error(exc)

    @property
    def consumers_count(self):
        return len(self.__consumers)

    async def __remove_expired_messages(self):
        delay = self.ttl / 10
        with suppress(asyncio.CancelledError):
            while True:
                expiration_time = time.time() - self.ttl
                while True:
                    try:
                        # noinspection PyUnresolvedReferences
                        message, put_time = self._queue[0]
                        if put_time < expiration_time:
                            self.get_nowait()
                        else:
                            break
                    except IndexError:
                        break
                await asyncio.sleep(delay)


class QueueMessage:
    __slots__ = ('body', '__queue')

    def __init__(self, body: Any, queue: DynamicBoundedManualAcknowledgeQueue):
        self.body = body
        self.__queue = queue

    def ack(self):
        self.__queue.ack()

    def nack(self):
        self.__queue.nack()

    def reject(self):
        raise NotImplementedError
