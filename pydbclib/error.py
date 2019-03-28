# -*- coding: utf-8 -*-
class ConnectError(Exception):
    def __init__(self, message):
        super().__init__(message)


class ExecuteError(Exception):
    def __init__(self, message):
        super().__init__(message)


class ArgsError(Exception):
    def __init__(self, message):
        super().__init__(message)
