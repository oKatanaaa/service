"""
Класс реализующий так называемое сообщение - позволяет передать иинформацию о каком-либо
событии адресату
"""


class Message:

    def __init__(self, whence, to, msg, options=None):
        """ Откуда - отправитель сообщения """
        self.whence = whence
        """ Куда - адресат """
        self.to = to
        """ Само сообщение """
        self.msg = msg
        """ Дополнительные параметры """
        self.options = options
