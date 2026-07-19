"""Streaming intake gRPC server support for Sutradhara.

The package owns only the machine-to-server streaming ingress. Completed bags
are handed to ``sutra intake watch`` for verification, catalog registration, and
terminal marker publication.
"""
