import asyncio
from typing import List
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, call

from callee import Matcher

from asyncbatcher import AsyncBatcher


class TestAsyncBatcher(IsolatedAsyncioTestCase):

    async def test_single_batch(self):
        values = {
            1: "foo",
            2: "bar",
            3: "hello",
            4: "world",
            5: "!"
        }

        loader = AsyncMock()
        loader.return_value = values

        batcher = AsyncBatcher(event_loop=asyncio.get_event_loop(),
                               loader=loader,
                               max_batch_size=5,  # Batch size allows all elements to be batched together
                               max_batch_time=2)  # Batch time allows all elements to be batched together

        futures = []
        # Batch up all items
        for key in values.keys():
            futures.append(batcher.get(key))

        result = await asyncio.gather(*futures)

        self.assertEqual(list(values.values()), list(result))
        loader.assert_called_once_with(ListMatcher([1, 2, 3, 4, 5]))

    async def test_multiple_batches_from_max_size(self):
        values = {
            1: "foo",
            2: "bar",
            3: "hello",
            4: "world",
            5: "!"
        }

        loader = AsyncMock()
        loader.return_value = values

        batcher = AsyncBatcher(event_loop=asyncio.get_event_loop(),
                               loader=loader,
                               max_batch_size=3,  # Batch size forces elements to be split into multiple batches
                               max_batch_time=2)

        futures = []
        for key in values.keys():
            futures.append(batcher.get(key))

        result = await asyncio.gather(*futures)

        self.assertEqual(list(values.values()), list(result))
        loader.assert_has_calls([
            call(ListMatcher([1, 2, 3])),
            call(ListMatcher([4, 5]))
        ])

    async def test_multiple_batches_from_max_time(self):
        values1 = {
            1: "foo",
            2: "bar",
            3: "hello"
        }
        values2 = {
            4: "world",
            5: "!"
        }

        loader = AsyncMock()
        loader.side_effect = [
            values1,  # First return value
            values2  # Second return value
        ]

        batcher = AsyncBatcher(event_loop=asyncio.get_event_loop(),
                               loader=loader,
                               max_batch_size=5,  # Batch size would allow all elements to be batched together
                               max_batch_time=2)  # But the sleep below will force items to be split into multiple batches

        futures = []
        for key in values1.keys():
            futures.append(batcher.get(key))

        await asyncio.sleep(2.1)  # Sleep exceeds max batch time

        for key in values2.keys():
            futures.append(batcher.get(key))

        result = await asyncio.gather(*futures)
        self.assertEqual(list(values1.values()) + list(values2.values()), list(result))
        loader.assert_has_calls([
            call(ListMatcher([1, 2, 3])),
            call(ListMatcher([4, 5]))
        ])

    async def test_default_values(self):
        loader = AsyncMock()
        loader.return_value = {
            1: "foo"
        }

        batcher = AsyncBatcher(event_loop=asyncio.get_event_loop(),
                               loader=loader,
                               max_batch_size=5,
                               max_batch_time=0.5,
                               default_value="hello world")

        self.assertEqual("foo", await batcher.get(1))
        self.assertEqual("hello world", await batcher.get(2))  # Not returned by the loader; defaults
        self.assertEqual("hello world", await batcher.get(3))  # Not returned by the loader; defaults


class ListMatcher(Matcher):
    """Utility matcher for list values."""

    def __init__(self, expected: List):
        self.expected = expected

    def match(self, value):
        return self.expected == list(value)

    def __repr__(self):
        return f"{self.expected}"
