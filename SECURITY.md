# Security

## Reporting a vulnerability

Please do not open a public issue for a security problem. Use GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository instead. You should get an acknowledgement within a few days.

Please include what you found, how to reproduce it, and what an attacker gains.

## What is in scope

PeerCache runs inside your application and talks to its peers over the network,
so the surfaces that matter are the ones a peer — or something pretending to be
one — can reach:

- **The peer protocol.** Every frame is validated before a decoder sees it, and
  decoders are bounded; a message that makes a node allocate, loop or panic out
  of proportion to its size is a vulnerability.
- **Mutual TLS** in the `transport` package: certificate verification on both
  sides and the TLS version floor.
- **Gossip.** Membership runs over hashicorp/memberlist; how PeerCache configures
  and consumes it is in scope, memberlist itself is reported upstream.

## What is not

PeerCache assumes every pod in a cluster is trusted and runs the same binary. A
node that is a legitimate member can read and write any key, by design. Network
isolation of the cluster is the deployment's job; see `deploy/README.md`.

## Supported versions

Until 1.0 only the latest release receives fixes.
