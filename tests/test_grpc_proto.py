"""Generated intake proto smoke tests."""

from __future__ import annotations

from sutra_agent._proto import device_pb2 as agent_device_pb2
from sutra_agent._proto import intake_pb2 as agent_intake_pb2
from sutradhara._proto import device_pb2, intake_pb2


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


def test_device_proto_messages_round_trip_on_server_and_agent_stubs() -> None:
    command = device_pb2.ServerCommand(
        start_receive=device_pb2.StartReceive(
            command_id="cmd-1",
            card_id="card-1",
            artifactclass="s-masters",
            label="Card 1",
            idempotency_key="key-1",
        )
    )
    assert device_pb2.ServerCommand.FromString(command.SerializeToString()) == command

    snapshot = device_pb2.DeviceMessage(
        card_snapshot=device_pb2.CardSnapshot(
            cards=[
                device_pb2.Card(
                    card_id="card-1",
                    label="Card 1",
                    kind=device_pb2.CARD_KIND_CARD,
                    size_bytes=5,
                    status="available",
                )
            ]
        )
    )
    parsed = agent_device_pb2.DeviceMessage.FromString(snapshot.SerializeToString())
    assert parsed.card_snapshot.cards[0].kind == agent_device_pb2.CARD_KIND_CARD

    browse = device_pb2.ServerCommand(
        list_directory=device_pb2.ListDirectory(
            request_id="req-1",
            card_id="card-1",
            rel_path="DCIM",
        )
    )
    assert device_pb2.ServerCommand.FromString(browse.SerializeToString()) == browse
    agent_browse = agent_device_pb2.ServerCommand.FromString(browse.SerializeToString())
    assert agent_browse.list_directory.rel_path == "DCIM"

    listing = device_pb2.DeviceMessage(
        directory_listing=device_pb2.DirectoryListing(
            request_id="req-1",
            entries=[
                device_pb2.DirectoryEntry(
                    name="100MEDIA",
                    is_dir=True,
                    is_package=False,
                )
            ],
            status=device_pb2.DIR_STATUS_OK,
        )
    )
    assert device_pb2.DeviceMessage.FromString(listing.SerializeToString()) == listing
    agent_listing = agent_device_pb2.DeviceMessage.FromString(listing.SerializeToString())
    assert agent_listing.directory_listing.status == agent_device_pb2.DIR_STATUS_OK
