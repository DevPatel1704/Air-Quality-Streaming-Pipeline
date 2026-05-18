"""
Compatibility patch for faust-streaming + aiokafka 0.14.x.
Patches faust's Transport._get_controller_node to use aiokafka's own MetadataRequest.
Import this module BEFORE importing faust.
"""

try:
    from aiokafka.protocol.metadata import MetadataRequest as _AioMetadataRequest
    NEEDS_PATCH = True
except ImportError:
    NEEDS_PATCH = False
import typing
import collections

# Patch typing.OrderedDict for mode-streaming compatibility in Python 3.9+
if not hasattr(typing, 'OrderedDict'):
    typing.OrderedDict = collections.OrderedDict

async def _patched_get_controller_node(self, owner, client, timeout=30000):
    """
    Replacement for faust Transport._get_controller_node that uses
    aiokafka's own MetadataRequest_v1 (which has .prepare()) instead of
    kafka-python's (which does not).
    """
    from faust.exceptions import NotReady

    nodes = [broker.nodeId for broker in client.cluster.brokers()]
    for node_id in nodes:
        if node_id is None:
            raise NotReady("Not connected to Kafka Broker")
        # Use aiokafka's own protocol request — it has the .prepare() method
        request = _AioMetadataRequest([])
        wait_result = await owner.wait(
            client.send(node_id, request),
            timeout=timeout,
        )
        if wait_result.stopped:
            owner.log.info("Shutting down - skipping creation.")
            return None
        response = wait_result.result
        return response.controller_id
    raise Exception("Controller node not found")


def apply():
    """Apply the monkey-patch to faust's Transport class."""
    if not NEEDS_PATCH:
        return
        
    import faust.transport.drivers.aiokafka as _drv
    import types

    _drv.Transport._get_controller_node = types.MethodType(
        _patched_get_controller_node,
        _drv.Transport,
    )
    print("Applied aiokafka compatibility patch.")
