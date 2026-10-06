# Metadata read buffers

`VerificationContext::read_to_end_bounded` materializes bounded metadata, such
as directory payloads, during verification. It used a 64 KiB scratch buffer for
every payload, so even an empty or one-byte object paid for zeroing 64 KiB. The
production helper now sizes its scratch buffer from the reader's exact length
hint, at most 64 KiB, and switches to 64 KiB reads when the hint understates
the payload. The hint never decides EOF or the limit, and the returned allocation
never exceeds the metadata limit.

## Measurements

`metadata_verification` reads every payload size with an exact length hint,
without one, and, above one byte, with a one-byte hint that understates it,
through three helpers in one executable. `scratch_64k` is the previous
production helper. `hybrid` is the rejected alternative: it reads known
payloads up to 64 KiB straight into the output and then probes EOF. Each case
is checked for exact bytes, identity, size, EOF and the 64 KiB read cap, and an
exact hint must size production scratch space to the payload. The run was
repeated with the helper order reversed. The table gives the previous helper's
time in microseconds from the forward run, then each helper relative to it as
forward / reverse.

| Hint | Bytes | `scratch_64k` µs | `hybrid` | `production` |
|---|---:|---:|---:|---:|
| known | 0 | 1.00 | 0.37 / 0.36 | 0.38 / 0.37 |
| known | 1 | 0.96 | 0.35 / 0.35 | 0.34 / 0.34 |
| known | 128 | 1.00 | 0.38 / 0.39 | 0.39 / 0.38 |
| known | 1,024 | 1.95 | 0.65 / 0.65 | 0.66 / 0.67 |
| known | 16,384 | 4.30 | 0.81 / 0.80 | 0.86 / 0.87 |
| known | 65,535 | 18.47 | 0.94 / 0.94 | 1.00 / 1.00 |
| known | 65,536 | 13.90 | 0.93 / 0.92 | 1.00 / 1.00 |
| known | 65,537 | 14.17 | 1.00 / 1.00 | 1.00 / 1.00 |
| known | 262,144 | 54.65 | 1.00 / 1.00 | 1.00 / 1.00 |
| unknown | 0 | 1.02 | 1.00 / 1.00 | 1.00 / 1.00 |
| unknown | 1 | 0.97 | 1.00 / 1.00 | 0.99 / 1.00 |
| unknown | 128 | 0.99 | 1.02 / 1.01 | 1.02 / 1.00 |
| unknown | 1,024 | 1.95 | 0.99 / 1.00 | 0.99 / 1.00 |
| unknown | 16,384 | 4.25 | 1.00 / 1.00 | 1.00 / 1.00 |
| unknown | 65,535 | 18.48 | 1.00 / 1.00 | 1.00 / 1.00 |
| unknown | 65,536 | 13.90 | 1.00 / 1.00 | 1.00 / 1.00 |
| unknown | 65,537 | 14.16 | 1.00 / 1.00 | 1.00 / 1.00 |
| unknown | 262,144 | 54.85 | 1.00 / 1.00 | 0.99 / 1.00 |
| understated | 128 | 1.00 | 1.15 / 1.17 | 1.19 / 1.18 |
| understated | 1,024 | 1.94 | 1.08 / 1.08 | 1.10 / 1.11 |
| understated | 16,384 | 4.26 | 1.94 / 1.93 | 1.93 / 1.94 |
| understated | 65,535 | 18.47 | 1.22 / 1.23 | 1.23 / 1.23 |
| understated | 65,536 | 13.92 | 1.32 / 1.32 | 1.32 / 1.33 |
| understated | 65,537 | 14.18 | 1.31 / 1.31 | 1.31 / 1.32 |
| understated | 262,144 | 54.69 | 1.32 / 1.33 | 1.31 / 1.33 |

With an exact hint, production takes 0.34 to 0.39 times as long as the
previous helper up to 128 bytes, 0.66 to 0.67 at 1 KiB and 0.86 to 0.87 at
16 KiB, and the
same time from 64 KiB, where both use a 64 KiB buffer. Without a hint it keeps
the 64 KiB buffer and is unchanged. `hybrid` is a little faster from 16 to
64 KiB (0.80 to 0.94) because it skips the copy from scratch space, but it sizes
the returned allocation from an unverified hint, so it was not adopted.

An understated hint breaks the `exact_len` contract. Production then makes two
one-byte reads, one that fills the hinted length and one that exceeds it,
before switching to 64 KiB reads, and takes 1.10 to 1.94 times as long as the
previous helper, which ignores hints. `hybrid` trusts the hint the same way
and costs the same. Two small reads cannot account for microseconds at 16 KiB,
and the cause was not isolated. A likely one is that the rest of the payload is
hashed from an unaligned offset: the previous helper itself takes about 1.33
times as long at 65,535 bytes as at 65,536.

`results.json` holds both runs, including each case's preflight check and the
largest buffer it supplied to the reader.

## Build and host

`results.json` records the `crates/casita` tree it measured, the build
command, compiler, lockfile and executable digests, the host, and the load
average before each run.

## Reproduce

```sh
taskset -c 32-47 cargo bench --no-default-features --features experimental --bench metadata_verification
CASITA_METADATA_READ_REVERSE=1 taskset -c 32-47 cargo bench --no-default-features --features experimental --bench metadata_verification
```
