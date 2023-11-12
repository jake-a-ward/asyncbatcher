import asyncio
import logging
import queue
import threading
import time
from asyncio import Future, AbstractEventLoop
from collections import defaultdict
from typing import List, Hashable, Callable, Dict, Iterable, Any


# Internal class for storing batch items
class _BatchItem:
    def __init__(self,
                 key: Hashable,
                 future: Future):
        self.key = key
        self.future = future


class AsyncBatcher:
    """
    Combine requests into batches.
    """
    LOGGER = logging.getLogger(__name__)

    def __init__(self,
                 event_loop: AbstractEventLoop,
                 loader: Callable[[Iterable[Hashable]], Dict[Hashable, Any]],
                 default_value=None,
                 max_batch_time=0.5,
                 max_batch_size=25):
        """
        Create an AsyncBatcher.

        :param event_loop: the event loop that should be used to execute async tasks; the event loop used by the loader function
        :param loader: an async function used to load multiple keys; (keys: Iterable[K]) -> Dict[K, V]
        :param default_value: the value to use when the loader does not return a value for a key
        :param max_batch_time: the maximum time to wait (in seconds) when generating a batch
        :param max_batch_size: the maximum number of items passed to the loader
        """
        self.event_loop = event_loop
        self.loader = loader
        self.default_value = default_value
        self.max_batch_time = max_batch_time
        self.max_batch_size = max_batch_size
        self.item_queue = queue.Queue()
        threading.Thread(target=self.__worker, daemon=True).start()

    def get(self, key: Hashable) -> Future:
        """
        Get the value for a key.

        :param key: the key to load (by passing to the loader in a batch)
        :return: a Future that will complete with the value loaded for the given key
        """
        future = Future()
        item = _BatchItem(key, future)
        self.__add_item(item)
        return future

    def __add_item(self, item: _BatchItem):
        self.item_queue.put(item)

    def __worker(self):
        while True:
            # Block until a batch of items is available
            batch = self.__deque_items_batch()

            if len(batch) > 0:
                keys_str = ','.join(map(lambda i: str(i.key), batch))
                AsyncBatcher.LOGGER.debug(f"Processing batch of size {len(keys_str)} with keys: {keys_str}")
                # Schedule the processing of the batch on the event loop; asyncio.run_coroutine_threadsafe is used for
                # doing this from a different thread than the one where the event loop is running
                asyncio.run_coroutine_threadsafe(self.__process_batch(batch), loop=self.event_loop)

    def __deque_items_batch(self) -> List[_BatchItem]:
        """
        Generate a list of batch items to process. Blocks for a period of time.

        Limited to whichever of these happens first:
            A maximum size defined by *max_batch_size*
            A maximum time span defined by *max_batch_time*

        :return: a list of BatchItem
        """
        start_time = time.time()
        end_time = start_time + self.max_batch_time
        batch = []
        while time.time() < end_time and len(batch) < self.max_batch_size:
            try:
                item = self.item_queue.get(block=True, timeout=0.25)
                batch.append(item)
            except queue.Empty:
                pass

        return batch

    async def __process_batch(self, batch: List[_BatchItem]):
        """
        Process a list of batch items.
        """
        index = defaultdict(list)
        for item in batch:
            index[item.key].append(item)

        try:
            # Dict[key, result]
            results = await self.loader(index.keys())

            # Ensure that all original keys are processed
            for key in index.keys():
                items = index[key]
                value = results[key] if key in results.keys() else self.default_value
                for item in items:
                    self.event_loop.call_soon_threadsafe(item.future.set_result, value)
        except Exception as e:
            # Forward the exception from the loader execution to all the futures in the batch
            for item in batch:
                self.event_loop.call_soon_threadsafe(item.future.set_exception, e)
