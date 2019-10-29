import asyncio
from uuid import uuid4
from typing import DefaultDict, Dict, List, Any, Hashable, Callable, Optional

from aiobroker.exceptions import ExchangeDoesNotExist
from aiobroker.queue import DynamicBoundedManualAcknowledgeQueue as Queue
from aiobroker.queue import ConsumerTag, RANDOM_NAME, UNLIMITED


Subscriber = Callable[[Any], Any]
ExchangeName = str
RoutingKey = Hashable
Queues = Dict[RoutingKey, Queue]
Exchanges = DefaultDict[ExchangeName, Queues]
Subscribers = DefaultDict[ExchangeName, Dict[RoutingKey, List[Subscriber]]]
Notifiers = DefaultDict[ExchangeName, Dict[RoutingKey, asyncio.Future]]


DEFAULT = 'default'


class Consumer:

    __slots__ = ('exchange', 'key', 'queue')

    def __init__(self, exchange: 'Exchange', key: Hashable, queue: Queue):
        self.exchange = exchange
        self.key = key
        self.queue = queue


class Exchange:
    def __init__(self, name: str):
        self.name: str = name
        self.queues: Dict[Hashable, List[Queue]] = {}

    async def publish(self, msg: Any, key: Optional[Hashable] = None):
        if key is None:
            key = type(msg)
        queues = self.queues.get(key, ())
        if queues:
            coros = [queue.put(msg) for queue in queues]
            await asyncio.gather(*coros)

    def publish_nowait(self, msg: Any, key: Optional[Hashable] = None):
        if key is None:
            key = type(msg)
        queues = self.queues.get(key, ())
        for queue in queues:
            queue.put_nowait(msg)

    def bind_queue(self, key: Hashable, queue: Queue):
        for existing_queue in self.queues.setdefault(key, []):
            if existing_queue.name == queue.name:
                return
        else:
            self.queues[key].append(queue)

    def unbind_queue(self, key: Hashable, queue: Queue):
        existing_queues = self.queues.get(key, [])
        if existing_queues:
            existing_queues.remove(queue)

    # def get_queue(self, key: Hashable, name: str) -> Optional[Queue]:
    #     for queue in self.queues.get(key, ()):
    #         if queue.name == name:
    #             return queue

    def release(self):
        self.queues = {}


class Broker:
    def __init__(self):
        self.exchanges: Dict[Hashable, Exchange] = {}
        self.queues: Dict[Hashable, Queue] = {}
        self.consumers: Dict[ConsumerTag, Consumer] = {}

    def add_consumer(self,
                     key: Hashable,
                     consumer: Callable[[Any], Any],
                     auto_ack: bool = True,
                     queue_name: str = RANDOM_NAME,
                     exchange_name: str = DEFAULT,
                     max_queue_size: int = UNLIMITED,
                     prefetch_count: int = UNLIMITED,
                     auto_delete: bool = True,
                     ttl: int = None,
                     flow_control: bool = True,
                     on_error: Optional[Callable[[Exception], Any]] = None) \
            -> ConsumerTag:

        # TODO: отдавать сущность Consumer, через которую можно отменять
        exchange = self.declare_exchange(exchange_name)
        queue = self.declare_queue(
            name=queue_name or str(uuid4()),
            prefetch_count=prefetch_count,
            auto_delete=auto_delete,
            maxsize=max_queue_size,
            ttl=ttl,
            flow_control=flow_control,
        )
        exchange.bind_queue(key, queue)
        consumer_tag = queue.basic_consume(consumer, auto_ack, on_error)
        self.consumers[consumer_tag] = Consumer(exchange, key, queue)
        return consumer_tag

    def cancel_consumer(self, tag: ConsumerTag):
        consumer = self.consumers.pop(tag, None)
        if consumer:
            queue = consumer.queue
            queue.basic_cancel(tag)
            if queue.auto_delete and queue.consumers_count == 0:
                self.queues.pop(queue.name, None)
                consumer.exchange.unbind_queue(consumer.key, queue)

    def declare_exchange(self, name: str) -> Exchange:
        exchange = self.exchanges.get(name)
        if not exchange:
            exchange = Exchange(name)
            self.exchanges[name] = exchange
        return exchange

    def declare_queue(self, name: str, **props) -> Queue:
        queue = self.queues.get(name)
        if not queue:
            queue = Queue(name=name, **props)
            self.queues[name] = queue
        return queue

    async def delete_exchange(self, name: str):
        try:
            exchange = self.exchanges.pop(name)
            await exchange.release()
        except KeyError:
            raise ExchangeDoesNotExist(name)

    def stop(self):
        for exchange in self.exchanges.values():
            exchange.release()
        for queue in self.queues.values():
            queue.close()
        self.exchanges = {}
        self.queues = {}
        self.consumers = {}

    def consume(self,
                key: Hashable,
                queue_name: str = None,
                exchange_name: str = DEFAULT,
                max_queue_size: int = UNLIMITED,
                flow_control: bool = True):

        # TODO: возвращать асинхронный контекстный менеджер (консьюмер)
        #  и удалять все созданное после выхода из него
        raise NotImplementedError

    async def publish(self,
                      msg: Any,
                      key: Optional[Hashable] = None,
                      exchange_name: str = DEFAULT):
        try:
            exchange = self.exchanges[exchange_name]
        except KeyError:
            raise ExchangeDoesNotExist(exchange_name)

        await exchange.publish(msg, key)

    def publish_nowait(self,
                       msg: Any,
                       key: Optional[Hashable] = None,
                       exchange_name: str = DEFAULT):

        try:
            exchange = self.exchanges[exchange_name]
        except KeyError:
            raise ExchangeDoesNotExist(exchange_name)

        exchange.publish_nowait(msg, key)


broker = Broker()


__all__ = [
    'Broker',
    'broker',
]
