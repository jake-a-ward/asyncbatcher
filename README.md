# asyncbatcher

Batch individual async calls into groups

## Example

```python
import asyncio
from typing import Iterable, Dict

from asyncbatcher import AsyncBatcher


async def load_data(ids: Iterable[int]) -> Dict[int, str]:
    # Load batch of data for multiple keys
    pass


batcher = AsyncBatcher(event_loop=asyncio.get_event_loop(),
                       loader=load_data,
                       default_value="default value",
                       max_batch_size=50,
                       max_batch_time=0.5)

v1 = batcher.get(1)
v2 = batcher.get(2)
v3 = batcher.get(3)

# Only single call to `load_data` is made

```