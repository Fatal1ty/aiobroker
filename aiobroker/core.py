import asyncio
import logging
from collections import defaultdict

from .common import short_type_name
from .queue import DynamicBoundedManualAcknowledgeQueue


class Broker:
    def __init__(self):
        self.exchanges = defaultdict(dict)
        self.subscribers = defaultdict(dict)
        self.notifiers = defaultdict(dict)
        self.notify_tasks = set()

        self.__is_stopping = False
        self.log = logging.getLogger('message-broker')

    async def stop(self):
        self.__is_stopping = True
        for exchange in self.notifiers.values():
            for notifier in exchange.values():
                notifier.cancel()
        for notify_task in self.notify_tasks:
            notify_task.cancel()
        self.log.debug('Message broker stopped')

    def is_stopping(self):
        return self.__is_stopping

    def subscribe(self, subscriber, key, exchange='default', maxsize=0,
                  flow_control=True):
        subscribers = self.subscribers[exchange].get(key)
        if subscribers:
            subscribers.append(subscriber)
        else:
            self.subscribers[exchange][key] = [subscriber]
            self._bind_queue(exchange, key, maxsize, flow_control)
            self._add_notifier(exchange, key)

    def unsubscribe(self, subscriber, key, exchange='default'):
        subscribers = self.subscribers[exchange].get(key)
        if not subscribers:
            raise RuntimeError(
                f'There is no subscriber {subscriber} for {exchange}.{key}')
        subscribers.remove(subscriber)
        if not subscribers:
            self._unbind_queue(exchange, key)
            self._remove_notifier(exchange, key)

    async def publish(self, msg, key=None, exchange='default'):
        if key is None:
            key = type(msg)
        queue = self.exchanges[exchange].get(key)
        if queue:
            await queue.put(msg)

    def publish_nowait(self, msg, key=None, exchange='default'):
        if key is None:
            key = type(msg)
        queue = self.exchanges[exchange].get(key)
        if queue:
            queue.put_nowait(msg)

    def _add_notifier(self, exchange, key):
        notifier = self.notify_loop(exchange, key)
        self.notifiers[exchange][key] = asyncio.ensure_future(notifier)

    def _remove_notifier(self, exchange, key):
        notifier = self.notifiers[exchange].pop(key)
        if notifier:
            notifier.cancel()

    def _bind_queue(self, exchange, key, maxsize, flow_control):
        queue = DynamicBoundedManualAcknowledgeQueue(maxsize, flow_control)
        self.exchanges[exchange][key] = queue

    def _unbind_queue(self, exchange, key):
        self.exchanges[exchange].pop(key)

    async def _notify(self, callbacks, queue, exchange, key):
        notify_task = asyncio.gather(*callbacks)
        try:
            self.notify_tasks.add(notify_task)
            await notify_task
            queue.ack()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            queue.nack()
            self.log.error(
                f'Failed to process message {exchange}.{key}: '
                f'one of subscribers raises {short_type_name(e)}')
        finally:
            self.notify_tasks.remove(notify_task)

    async def notify_loop(self, exchange, key):
        queue = self.exchanges[exchange][key]
        while not self.is_stopping():
            msg = await queue.get()
            callbacks = [subscriber(msg) for subscriber
                         in self.subscribers[exchange].get(key, [])]
            if callbacks:
                asyncio.ensure_future(
                    self._notify(callbacks, queue, exchange, key))


broker = Broker()


__all__ = [
    'Broker',
    'broker',
]
