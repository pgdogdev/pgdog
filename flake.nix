{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
  };
  
  outputs = { self, nixpkgs, flake-utils, crane }: 
    flake-utils.lib.eachSystem flake-utils.lib.allSystems (system: 
      let
        pkgs = import nixpkgs { inherit system; };
        craneLib = crane.mkLib pkgs;
        stdenv' = p: p.stdenvAdapters.withCFlags [ "-O" ] (p.stdenvAdapters.useMoldLinker p.clangStdenv);
        stdenv = stdenv' pkgs;

        devShell = craneLib.devShell.override {
          mkShell = pkgs.mkShell.override {
            inherit stdenv;
          };
        };

        env = {
          LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = "${stdenv.cc}/bin/cc";
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS = "-C link-arg=--ld-path=${stdenv.cc}/bin/ld";
        };
        
        commonArgs = {
          src = let
            unfilteredSrc = ./.;
            fs = pkgs.lib.fileset;
          in fs.toSource {
            root = unfilteredSrc;
            fileset = fs.unions [
              (craneLib.fileset.cargoTomlAndLock unfilteredSrc)
              (craneLib.fileset.rust unfilteredSrc)
              (fs.fileFilter
                (file: file.hasExt "c" || file.hasExt "h" || file.hasExt "sql")
                unfilteredSrc
              )
            ];
          };
          strictDeps = true;

          stdenv = stdenv';
          nativeBuildInputs = with pkgs; [
            pkg-config
          ];
          buildInputs = with pkgs; [
            openssl
          ];

          inherit env;
        } // (craneLib.crateNameFromCargoToml { cargoToml = ./pgdog/Cargo.toml; });

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        pgDog = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          doCheck = false;
          cargoExtraArgs = "-p pgdog";
        });

      in {
        packages.default = pgDog;

        devShells.default = devShell {
          checks = self.checks;
          inputsFrom = [ cargoArtifacts ];
          inherit env;
        };

        checks = {
          inherit pgDog;

          pgDogClippy = craneLib.cargoClippy (commonArgs // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets --all-features -- --deny warnings";
          });
          
          pgDogFmt = craneLib.cargoFmt commonArgs;
          
          pgDogNextest = craneLib.cargoNextest (commonArgs // {
            inherit cargoArtifacts;
            checkPhaseCargoCommand = "echo hello world";
            cargoNextestExtraArgs = "--test-threads=1 --no-fail-fast";
          });
        };
      }
    );
}
