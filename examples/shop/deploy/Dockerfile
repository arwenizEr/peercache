# The shop and its load generator, built on the host (see m16.sh) and copied in.
# Alpine rather than distroless so the scenarios can kubectl exec and kubectl cp
# the load generator's ledger out of its pod.
FROM alpine:3.20
COPY shop shopload /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/shop"]
