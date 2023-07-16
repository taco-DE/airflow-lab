from abc import ABCMeta, abstractmethod


class Notifier(metaclass=ABCMeta):
    def __init__(self):
        ...  # TODO

    @abstractmethod
    def build_msg(self, content: dict) -> dict:
        ...

    @abstractmethod
    def send_msg(self, content: dict) -> None:
        ...
