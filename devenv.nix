{ pkgs, lib, ... }:

let
  # RustFS 1.0.1 is not in devenv-nixpkgs yet, and 1.0.0-rc.1 there reports
  # healthy before it serves S3. Use the release binaries, the same artifacts
  # Windows CI installs, until pkgs.rustfs catches up. The hashes are the
  # digests GitHub publishes for each asset.
  rustfsVersion = "1.0.1";
  rustfsAssets = {
    x86_64-linux = {
      name = "rustfs-linux-x86_64-musl-v${rustfsVersion}.zip";
      sha256 = "a834096dafa1f1a55825a2cdaf49d006a193978d344f2d508c2be475133738a3";
    };
    aarch64-linux = {
      name = "rustfs-linux-aarch64-musl-v${rustfsVersion}.zip";
      sha256 = "d2533e293204597416cb8d30790ea35df14cb4521633fa3574c64333141bafdf";
    };
    aarch64-darwin = {
      name = "rustfs-macos-aarch64-v${rustfsVersion}.zip";
      sha256 = "18aac7101c3484b98f93de64a0609aaa8e02aba072ba7927172c4b2eae7d4843";
    };
  };
  rustfsAsset = rustfsAssets.${pkgs.stdenv.hostPlatform.system} or null;
  rustfs =
    if rustfsAsset == null then
      pkgs.rustfs
    else
      pkgs.stdenvNoCC.mkDerivation {
        pname = "rustfs";
        version = rustfsVersion;
        src = pkgs.fetchurl {
          url = "https://github.com/rustfs/rustfs/releases/download/${rustfsVersion}/${rustfsAsset.name}";
          inherit (rustfsAsset) sha256;
        };
        nativeBuildInputs = [
          pkgs.unzip
        ]
        ++ lib.optionals pkgs.stdenv.hostPlatform.isDarwin [
          pkgs.cctools
          pkgs.darwin.autoSignDarwinBinariesHook
        ];
        unpackPhase = "unzip -q $src";
        installPhase = ''
          binary=$(find . -name rustfs -type f)
          install -Dm755 "$binary" $out/bin/rustfs
        ''
        + lib.optionalString pkgs.stdenv.hostPlatform.isDarwin ''
          # The macOS release links Homebrew's xz; the fixup re-signs it.
          install_name_tool -change /opt/homebrew/opt/xz/lib/liblzma.5.dylib \
            ${lib.getLib pkgs.xz}/lib/liblzma.5.dylib $out/bin/rustfs
        '';
        # The musl builds are static. On macOS the fixup only re-signs.
        dontFixup = !pkgs.stdenv.hostPlatform.isDarwin;
        dontStrip = true;
      };
in
{
  languages.rust.enable = true;
  # the casita-worker crate (Cloudflare Workers) compiles to wasm, which
  # needs a channel toolchain (the nixpkgs one cannot add targets).
  # x86_64-pc-windows-gnu type-checks the Windows cfg branches locally:
  #   cargo check --target x86_64-pc-windows-gnu --all-targets
  languages.rust.channel = "stable";
  # pin one exact toolchain for local dev and CI (no version matrix).
  languages.rust.version = "1.96.0";
  languages.rust.targets = [
    "wasm32-unknown-unknown"
    "x86_64-pc-windows-gnu"
  ];

  # Astro/Starlight documentation site under docs/. Keep dependency
  # installation explicit in that directory; CI uses npm ci against its lock.
  languages.javascript = {
    enable = true;
    package = pkgs.nodejs_24;
    npm.enable = true;
  };

  # Repository and benchmark tools; wrangler + worker-build build and run the
  # casita-worker crate locally (wrangler dev simulates R2 and D1). The worker
  # tooling is gated to Linux: wrangler does not build on macOS in nixpkgs, and
  # CI never touches the worker, so the macos test job does not need it.
  packages = [
    pkgs.git
    pkgs.lychee
    # One pinned environment owns the end-to-end harness and all comparison
    # binaries, so a publication run needs no nested shell or ad-hoc install.
    pkgs.python3
    pkgs.time
    pkgs.gnutar
    # Declare and resolve deployment credentials outside Casita's repository
    # graph. The application continues to consume the standard AWS environment
    # contract, so both S3 clients receive one SecretSpec resolution.
    pkgs.secretspec
    # The wal3 integration test starts this local S3-compatible server and
    # races independent Casita runners against its conditional manifest PUTs.
    rustfs
    # wal3's currently in-tree Chroma dependency graph generates protobuf
    # bindings while compiling `chroma-types`.
    pkgs.protobuf
    pkgs.zstd
    pkgs.restic
    pkgs.borgbackup
  ] ++ lib.optionals pkgs.stdenv.isLinux [
    pkgs.wrangler
    pkgs.worker-build
  ] ++ lib.optionals pkgs.stdenv.isDarwin [
    # Native FSKit needs a modern SDK; the default SDK lacks FSKit.framework.
    pkgs.apple-sdk_26
  ];

  # mingw cc for the Windows cargo check, exposed only through the
  # target-scoped variables: putting the cross cc in `packages` would run its
  # setup hook and point CC/CXX at mingw for host builds too.
  env.CC_x86_64_pc_windows_gnu = "${pkgs.pkgsCross.mingwW64.stdenv.cc}/bin/x86_64-w64-mingw32-gcc";
  env.CXX_x86_64_pc_windows_gnu = "${pkgs.pkgsCross.mingwW64.stdenv.cc}/bin/x86_64-w64-mingw32-g++";
  env.AR_x86_64_pc_windows_gnu = "${pkgs.pkgsCross.mingwW64.stdenv.cc.bintools.bintools}/bin/x86_64-w64-mingw32-ar";

  # clippy runs as a git-hook, on commit and in CI via `devenv test`. devenv
  # points it at the toolchain above and turns offline mode off so it can fetch
  # dependencies; the flags reproduce the old CI job
  # (cargo clippy --all-features --all-targets -- -D warnings). Formatting is
  # not a hook: the CI `fmt` job reformats and commits it back instead.
  git-hooks.hooks.clippy = {
    enable = true;
    settings = {
      allFeatures = true;
      denyWarnings = true;
      extraArgs = "--all-targets";
    };
  };

  # `devenv test` runs the clippy hook above, then this: the all-features suite
  # and the default-feature build (git and cli off) a downstream crate would get.
  enterTest = ''
    secretspec check --provider null --no-prompt \
      --reason "validate Casita secret declarations"
    cargo test --all-features
    cargo test
  '';

  processes.docs.exec = ''
    cd docs && npm run dev
  '';

  scripts.benchmark.exec = ''
    python3 -m benchmarks.cli "$@"
  '';

  scripts.git-scale-benchmark.exec = ''
    python3 -m benchmarks.suites.git "$@"
  '';

  scripts.benchmark-dashboard.exec = ''
    python3 -m benchmarks.dashboard "$@"
  '';
}
