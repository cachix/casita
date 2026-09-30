# Security Policy

## Supported versions

Casita has not published a stable release. During the pre-release period,
security fixes are made on `main` on a best-effort basis; a commit from `main`
is not a long-term support guarantee.

After the first release, this section will name the supported release lines.
Unless a release announcement says otherwise, only the newest patch release of
the newest release line will receive security fixes.

## Reporting a vulnerability

Email [security@cachix.org](mailto:security@cachix.org) with `Casita` in the
subject. This follows the published
[Cachix security policy](https://www.cachix.org/security). Do not open a public
issue for a suspected vulnerability before maintainers have had an opportunity
to assess it.

Include as much of the following as possible:

- the affected Casita version or commit;
- enabled Cargo features, backend profile, operating system, and architecture;
- a minimal reproducer or malformed input;
- the expected and observed behavior;
- the security impact and required attacker capabilities; and
- sanitized logs, backtraces, or repository metadata.

Do not include credentials, private repository contents, encryption keys, or
other secrets. Maintainers will acknowledge reports as capacity permits,
coordinate validation and remediation with the reporter, and request a CVE when
the impact warrants one. There is no response-time SLA before the first stable
release.

## Security boundaries

Reports are especially useful when Casita accepts invalid identity or linkage,
publishes an incomplete closure, escapes a selected filesystem root, discloses
repository data across a view or transfer boundary, corrupts committed state,
or exceeds a documented hostile-input resource limit.

The [reliability contract](docs/src/content/docs/reference/reliability.md)
lists the durability and crash-safety guarantees, where each applies, and their
known limits. A report that shows a clause violated outside its listed limits
is a bug; report it privately when it also crosses a boundary above.

Deployment authentication and secret management remain outside the generic
repository:

- SSH transport delegates authentication, encryption, host-key verification,
  agents, and proxy behavior to OpenSSH.
- S3-compatible storage uses the standard AWS credential chain. Credentials
  must not be embedded in repository URLs or stored in Casita objects.
- The experimental Git smart-HTTP server provides neither TLS nor client
  authentication. Its default loopback binding must remain behind an explicit
  trusted boundary when exposed remotely.
- The initial storage profiles do not promise encryption at rest or
  access-pattern hiding.

Experimental features may change and are not covered by a stable compatibility
promise, but security issues in them should still be reported privately.

## Disclosure and fixes

Security fixes should include a regression test whenever a safe reproducer can
be committed. Release notes must identify affected versions, impact,
mitigations, and whether stored data needs verification, repair, or migration.
Published tags are immutable: a faulty release is replaced by a new version,
never by moving or rewriting its tag.
