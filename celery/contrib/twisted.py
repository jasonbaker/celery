from threading import Event

from celery.task.http import (HttpDispatchTask, GET_METHODS, MutableURL, 
                              URL as BaseURL)
from celery.exceptions import TimeoutError
from twisted.web.client import getPage

class DeferredResult(object):
    """
    Make a result from a twisted deferred object.
    """
    def __init__(self, deferred):
        self.event = Event()
        self.deferred = deferred
        self.deferred.addCallback(self.handle_response)
        self.deferred.addErrback(self.handle_error)
        self.result = None
        self.traceback = None

    def revoke(self, *args, **kwargs):
        raise NotImplementedError, "Cannot revoke deferred results"

    @property
    def ready(self):
        return self.event.is_set()

    def unblock(self):
        """
        Unblock any threads that have called get or wait on this result.
        """
        self.event.set()

    def handle_response(self, message):
        """
        The deferred's callback.
        """
        self.result = message
        print message
        self.unblock()

    def handle_error(self, failure):
        """
        The deferred's errback.
        """
        self.result = failure.value
        self.traceback = failure.getTraceback()
        self.unblock()

    def wait(self, timeout=None):
        """
        Block until timeout seconds pass, the task is successful, or the task
        fails.
        """
        self.event.wait(timeout)
        if not self.ready:
            # The above wait returned, but we're not ready yet.  We must have
            # timed out.
            raise TimeoutError
        else:
            return self.result 

    get = wait

    def successful(self):
        return self.ready and not isinstance(self.result, Exception)

class TwistedHttpTask(HttpDispatchTask):
    @classmethod
    def apply_async(self, url=None, method='GET', args=None, kwargs=None, **options):
        """
        Run this task using Twisted's asynchronous HTTP client.
        """
        url = MutableURL(url)
        if method in GET_METHODS and kwargs:
            url.query.update(kwargs)
            postdata=None
        else:
            postdata=kwargs
        deferred = getPage(url=str(url), 
                           method=method,
                           postdata=postdata,
                           agent='Zero Webserver')
        return DeferredResult(deferred)

    @classmethod
    def delay(self, url=None, method='GET', *args, **kwargs):
        return self.apply_async(url=url, method=method, args=args,
                                kwargs=kwargs)
       
class URL(BaseURL):
    dispatcher = TwistedHttpTask
