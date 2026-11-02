"""Hermetic request/response contract tests; fake RPCs do not prove media effects."""
import uuid

import grpc
import pytest

from sutradhara._proto import layer5_pb2 as pb
from sutradhara.rem_maintenance import closeout_tape, supersede_tail

TAPE, KEY, OP = uuid.UUID(int=1), uuid.UUID(int=2), uuid.UUID(int=3)


class Refusal(grpc.RpcError):
    def __init__(self, message, code=grpc.StatusCode.FAILED_PRECONDITION):
        self.message, self.status = message, code

    def code(self): return self.status
    def details(self): return self.message


def test_generated_fields_and_service():
    service = pb.DESCRIPTOR.services_by_name['Catalog']
    assert service.methods_by_name['CloseoutTape'].input_type.name == 'CloseoutTapeRequest'
    assert pb.CloseoutTapeRequest.DESCRIPTOR.fields_by_name['idempotency_key'].number == 3
    assert pb.SupersedeTailRequest.DESCRIPTOR.fields_by_name['observed_tape_log_sequence'].number == 5
    assert pb.SupersedeTailResponse.DESCRIPTOR.fields_by_name['tape_log_sequence'].number == 4
    request = pb.SupersedeTailRequest(observed_tape_log_sequence=0)
    assert request.HasField('observed_tape_log_sequence')


def test_supersede_repreviews_and_reconfirms_changed_found_list():
    calls, confirmations = [], []
    class Catalog:
        def SupersedeTail(self, request, *, timeout):
            calls.append(request)
            if not request.HasField('confirm_supersede'):
                newer = len(calls) > 1
                return pb.SupersedeTailResponse(found=['new' if newer else 'old'],
                    required_confirmation='SUPERSEDE TAPE', tape_log_sequence=20 if newer else 10)
            if len(calls) == 2:
                raise Refusal('TailPreviewStale: tape-log sequence changed from 10 to 20; preview supersede-tail again.')
            assert request.observed_tape_log_sequence == 20
            assert list(request.expected_found) == ['new']
            return pb.SupersedeTailResponse(completed=True, tape_log_sequence=21)
    result = supersede_tail(Catalog(), TAPE, confirm=lambda p: confirmations.append(list(p.found)) or True)
    assert result.completed and confirmations == [['old'], ['new']]
    assert calls[1].observed_tape_log_sequence == 10
    assert all(r.tape_uuid == r.ack_tape_uuid == TAPE.bytes for r in calls)


@pytest.mark.parametrize('approve', [False, True])
def test_supersede_refusal_is_not_retried_or_confirmed_without_approval(approve):
    calls = []
    class Catalog:
        def SupersedeTail(self, request, **kwargs):
            calls.append(request)
            if len(calls) == 1: return pb.SupersedeTailResponse(required_confirmation='YES')
            raise Refusal('SessionSuffixContradiction: foreign suffix')
    with pytest.raises(Refusal if approve else PermissionError):
        supersede_tail(Catalog(), TAPE, confirm=lambda _: approve)
    assert len(calls) == (2 if approve else 1)


@pytest.mark.parametrize('already_protected', [False, True])
def test_closeout_due_noop_and_retry_preserve_request_identity(already_protected):
    calls = []
    class Catalog:
        def CloseoutTape(self, request, **kwargs):
            calls.append(request.SerializeToString())
            assert request.expected_pool_id == 'pool'
            assert request.idempotency_key.value == KEY.bytes
            return pb.OperationRef(operation_id=OP.bytes)
    class Daemon:
        def WatchOperation(self, request, **kwargs):
            assert request.operation_id == OP.bytes
            yield pb.OperationStatus(operation_id=OP.bytes, operation_kind='tape_closeout',
                                     state=pb.OPERATION_STATE_RUNNING)
            yield pb.OperationStatus(operation_id=OP.bytes, operation_kind='tape_closeout',
                state=pb.OPERATION_STATE_SUCCEEDED, progress={'already_protected': str(already_protected).lower(),
                    'source': 'rebuilt', 'phase': 'complete', 'w': '3', 't': '3'})
    for _ in range(2):
        result = closeout_tape(Catalog(), Daemon(), TAPE, expected_pool_id='pool', idempotency_key=KEY)
        assert result.progress['already_protected'] == str(already_protected).lower()
    assert calls[0] == calls[1]


def test_wrong_pool_refusal_is_preserved_without_watch():
    class Catalog:
        def CloseoutTape(self, request, **kwargs):
            assert request.expected_pool_id == 'wrong'
            raise Refusal("CloseoutTape expected_pool_id does not match the tape's assigned pool")
    with pytest.raises(Refusal, match='expected_pool_id does not match'):
        closeout_tape(Catalog(), None, TAPE, expected_pool_id='wrong', idempotency_key=KEY)


def test_nil_key_fails_before_rpc():
    with pytest.raises(ValueError, match='must not be the nil UUID'):
        closeout_tape(None, None, TAPE, idempotency_key=uuid.UUID(int=0))


@pytest.mark.parametrize('state', [pb.OPERATION_STATE_FAILED, pb.OPERATION_STATE_CANCELLED,
                                   pb.OPERATION_STATE_COMPLETION_UNKNOWN])
def test_closeout_failure_and_unknown_completion_preserve_diagnostic(state):
    class Catalog:
        def CloseoutTape(self, *a, **kw): return pb.OperationRef(operation_id=OP.bytes)
    class Daemon:
        def WatchOperation(self, *a, **kw):
            yield pb.OperationStatus(operation_id=OP.bytes, operation_kind='tape_closeout',
                                     state=state, error_summary='CloseoutCheckMismatch')
    with pytest.raises(RuntimeError, match='CloseoutCheckMismatch'):
        closeout_tape(Catalog(), Daemon(), TAPE, idempotency_key=KEY)
