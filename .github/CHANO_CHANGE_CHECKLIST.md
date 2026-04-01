# Chano Relay Governance Checklist

Use this checklist for every material Relay change.

## Before Implementation

- identify whether the change is Class A, B, or C
- identify governing Core Documents
- review 21 CHANO RELAY IMPLEMENTATION STANDARD.md
- create a durable implementation record
- define exact acceptance criteria and validation evidence

## During Implementation

- keep Relay non-authoritative for vault or projection truth
- preserve explicit routing gates and transport integrity
- keep pairing and session behavior control-plane only
- preserve reviewable neutrality and downstream bridge assumptions
- verify and document Redis capability assumptions when session atomicity behavior is touched

## Validation Minimums

Use the narrowest evidence that still proves the authoritative runtime path.

Typical evidence slices:

- relay runtime audit
- transport integrity audit
- pairing/session lifecycle tests
- projection bridge evidence when routing affects downstream behavior
- contention or multi-instance evidence when session exclusivity or degraded capability behavior changes

## Documentation And Closeout

- update 21 CHANO RELAY IMPLEMENTATION STANDARD.md if lasting practice changed
- update governance documents if the workflow or evidence rule changed
- use ACP if the change alters architecture, protocol, validation, execution, or boundary law
- do not close the change until validation evidence and the implementation record are complete
