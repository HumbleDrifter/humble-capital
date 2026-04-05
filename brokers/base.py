from abc import ABC, abstractmethod


class BrokerAdapter(ABC):
    name = "base"

    @abstractmethod
    def place_options_order(self, order):
        raise NotImplementedError


def broker_result(ok, **payload):
    return {
        "ok": bool(ok),
        **payload,
    }
