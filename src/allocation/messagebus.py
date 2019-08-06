from typing import List, Dict, Callable, Type
from allocation import events, handlers


def handle(events_: List[events.Event]):
    while events_:
        event = events_.pop(0)
        for handler in HANDLERS[type(event)]:
            handler(event)


HANDLERS = {
    events.BatchCreated: [handlers.add_batch],
    events.AllocationRequired: [handlers.allocate],
    events.OutOfStock: [handlers.send_out_of_stock_notification],
}  # type: Dict[Type[events.Event], List[Callable]]
