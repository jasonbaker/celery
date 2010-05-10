from celery import conf
from celery.utils import gen_unique_id
from celery.messaging import BroadcastPublisher, ControlReplyConsumer
from celery.messaging import with_connection, get_consumer_set


@with_connection
def discard_all(connection=None,
        connect_timeout=conf.BROKER_CONNECTION_TIMEOUT):
    """Discard all waiting tasks.

    This will ignore all tasks waiting for execution, and they will
    be deleted from the messaging server.

    :returns: the number of tasks discarded.

    """
    consumers = get_consumer_set(connection=connection)
    try:
        return consumers.discard_all()
    finally:
        consumers.close()


def revoke(task_id, destination=None, **kwargs):
    """Revoke a task by id.

    If a task is revoked, the workers will ignore the task and not execute
    it after all.

    :param task_id: Id of the task to revoke.
    :keyword destination: If set, a list of the hosts to send the command to,
        when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword connect_timeout: Timeout for new connection if a custom
        connection is not provided.
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.

    """
    return broadcast("revoke", destination=destination,
                               arguments={"task_id": task_id}, **kwargs)




def rate_limit(task_name, rate_limit, destination=None, **kwargs):
    """Set rate limit for task by type.

    :param task_name: Type of task to change rate limit for.
    :param rate_limit: The rate limit as tasks per second, or a rate limit
      string (``"100/m"``, etc. see :attr:`celery.task.base.Task.rate_limit`
      for more information).
    :keyword destination: If set, a list of the hosts to send the command to,
        when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword connect_timeout: Timeout for new connection if a custom
        connection is not provided.
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.

    """
    return broadcast("rate_limit", destination=destination,
                                   arguments={"task_name": task_name,
                                              "rate_limit": rate_limit},
                                   **kwargs)


@with_connection
def broadcast(command, arguments=None, destination=None, connection=None,
        connect_timeout=conf.BROKER_CONNECTION_TIMEOUT, reply=False,
        timeout=1, limit=None):
    """Broadcast a control command to the celery workers.

    :param command: Name of command to send.
    :param arguments: Keyword arguments for the command.
    :keyword destination: If set, a list of the hosts to send the command to,
        when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword connect_timeout: Timeout for new connection if a custom
        connection is not provided.
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.

    """
    arguments = arguments or {}
    reply_ticket = reply and gen_unique_id() or None


    broadcast = BroadcastPublisher(connection)
    try:
        broadcast.send(command, arguments, destination=destination,
                       reply_ticket=reply_ticket)
    finally:
        broadcast.close()

    if reply_ticket:
        crq = ControlReplyConsumer(connection, reply_ticket)
        try:
            return crq.collect(limit=limit, timeout=timeout)
        finally:
            crq.close()
