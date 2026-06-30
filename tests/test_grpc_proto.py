"""Generated intake proto smoke tests."""

from __future__ import annotations

from sutra_agent._proto import intake_pb2 as agent_intake_pb2
from sutradhara._proto import intake_pb2


def test_intake_proto_messages_round_trip_on_server_and_agent_stubs() -> None:
    start = intake_pb2.StartIntakeRequest(
        idempotency_key="key",
        artifactclass="video-master",
        source_kind="card",
        source_plan_digest="a" * 64,
    )
    assert intake_pb2.StartIntakeRequest.FromString(start.SerializeToString()) == start

    commit = intake_pb2.CommitIntakeRequest(
        intake_id="intake-1",
        files=[
            intake_pb2.ManifestEntry(
                relpath="clip.mov",
                client_sha256="b" * 64,
                bytes=5,
            )
        ],
        receive_facts=intake_pb2.ReceiveFacts(
            canonicalization_version="receive-bagit-path-v2",
            skipped_count=0,
            package_profile_version="package-tar-v1",
        ),
        package_indexes=[
            intake_pb2.PackageIndex(
                logical_member_path="A.fcpbundle",
                stored_member_path="A.fcpbundle.tar",
                sha256="c" * 64,
                members=[
                    intake_pb2.PackageMemberEntry(
                        member="A.fcpbundle",
                        type="directory",
                        length=0,
                    )
                ],
            )
        ],
        manifest_digest="d" * 64,
    )
    assert intake_pb2.CommitIntakeRequest.FromString(commit.SerializeToString()) == commit
    agent_start = agent_intake_pb2.StartIntakeRequest.FromString(start.SerializeToString())
    assert agent_start.source_plan_digest == "a" * 64
