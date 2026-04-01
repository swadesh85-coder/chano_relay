# Chano Relay Governance Review Policy

This repository follows Documents 17, 18, 19, and 21 as mandatory operational controls.

## Review Rules

- no material Relay change may be approved without a completed change record
- no material Relay change may be closed without validation evidence proving the authoritative runtime path
- if the change is Class B, repository-local green status is insufficient without combined cross-repository validation
- if the change is Class C, approval must stop until 3 CHANO ARCHITECTURAL CHANGE PROTOCOL.md is satisfied

## Blocking Conditions

Reject or block approval when any of the following are true:

- governing documents are not identified
- evidence depends on helper-backed proof or non-authoritative alternate paths
- relay neutrality, routing, or control-plane boundaries become ambiguous
- Redis atomicity assumptions changed without explicit capability verification or fallback decision
- required implementation-document updates were skipped
- the durable implementation record is missing

## Escalation

- ambiguity in Core Document meaning must be escalated through the architectural change protocol
- audit-triggering Relay changes must be handled under 17 CHANO ARCHITECTURE AUDIT METHODOLOGY.md
