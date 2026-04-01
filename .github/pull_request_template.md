# Chano Relay Change Record

Complete this template for every material change.

## 1. Change Class

- [ ] Class A: local implementation change
- [ ] Class B: cross-boundary implementation change
- [ ] Class C: architectural change

## 2. Repository Scope

- Primary repository: `chano_relay`
- Affected repositories:
  - [ ] `Chano`
  - [ ] `chano_relay`
  - [ ] `chano_web`

## 3. Governing Documents

List the governing Core Documents and implementation documents reviewed.

- Core Documents:
- Implementation Documents:

## 4. Problem Statement

Describe the issue being solved and why the current behavior is insufficient.

## 5. Intended Boundary Of Change

State exactly what is allowed to change and what must remain unchanged.

## 6. Acceptance Criteria

- [ ] relay neutrality preserved
- [ ] transport integrity preserved
- [ ] control-plane-only pairing/session behavior preserved
- [ ] Redis capability assumptions and atomicity behavior are explicitly preserved or updated
- [ ] downstream bridge behavior remains validated when routing changes
- [ ] affected implementation documents reviewed for currency

Additional acceptance criteria:

- 

## 7. Required Validation Evidence

List the exact commands, tasks, or evidence artifacts used.

- [ ] relay runtime audit
- [ ] relay transport integrity audit
- [ ] pairing/session lifecycle tests
- [ ] projection bridge evidence when downstream behavior is affected
- [ ] contention or multi-instance evidence when session atomicity behavior changes

Evidence used:

- 

## 8. Class B Coordination

Complete this section only for Class B changes.

- Cross-repository assumptions:
- Repository owners consulted:
- Rollout or merge order:
- Cross-repository validation:
- Rollback or containment plan:

## 9. Durable Implementation Record

Link the durable record required by 19 CHANO IMPLEMENTATION METHODOLOGY.md.

- Record location:

## 10. Documentation Updates

- [ ] no implementation document update required
- [ ] updated 21 CHANO RELAY IMPLEMENTATION STANDARD.md
- [ ] updated 18 CHANO IMPLEMENTATION GOVERNANCE MODEL.md
- [ ] updated 19 CHANO IMPLEMENTATION METHODOLOGY.md
- [ ] architectural change process required under 3 CHANO ARCHITECTURAL CHANGE PROTOCOL.md

## 11. Reviewer Closeout

- [ ] change class is correct
- [ ] governing documents are correct
- [ ] required evidence is present
- [ ] repository ownership is preserved
- [ ] completion gates from Documents 18 and 19 are satisfied
