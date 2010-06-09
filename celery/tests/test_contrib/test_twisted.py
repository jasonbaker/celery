# This import *must* come first or the world will end.  Currently, this will not
# raise an exception if twisted isn't installed, so we can still handle that
# case below.
from nose.twistedtools import deferred

try:
    import twisted
except ImportError:
    from nose.tools.skip import SkipTest
    import warnings
    warnings.warn('Twisted not installed.  Skipping twisted tests')
    raise SkipTest

from functools import wraps

from twisted.internet import defer, reactor
from celery.contrib.twistedtools import DeferredResult

@deferred(timeout=2)
def test_deferred_success():
    d = defer.Deferred()
    result = DeferredResult(d)

    def check_result(message):
        assert result.result == message
    d.addCallback(check_result)
    reactor.callLater(1, d.callback, 'Hello, world!')
    return d
