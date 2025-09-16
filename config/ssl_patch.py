import ssl
import aiohttp
import os


def patch_ssl_correctly():
    """
    Обход проверки SSL сертификатов
    """
    os.environ['PYTHONHTTPSVERIFY'] = '0'

    ssl._create_default_https_context = ssl._create_unverified_context

    original_init = aiohttp.TCPConnector.__init__

    def new_init(self, *args, **kwargs):
        kwargs.pop('verify_ssl', None)
        kwargs.pop('ssl_context', None)
        kwargs.pop('fingerprint', None)

        kwargs['ssl'] = False

        return original_init(self, *args, **kwargs)

    aiohttp.TCPConnector.__init__ = new_init
