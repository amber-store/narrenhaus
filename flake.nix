{
  description = "narrenhaus";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

    systems.url = "github:nix-systems/default";

    gonixgo = {
      url = "github:draganm/gonixgo/v0.5.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.systems.follows = "systems";
    };

  };

  outputs = { self, nixpkgs, systems, gonixgo, ... }:
    let
      eachSystem = f:
        nixpkgs.lib.genAttrs (import systems)
        (system: f system nixpkgs.legacyPackages.${system});
    in {

      # Evaluating a gonixgo package runs `gonixgo resolve` through
      # builtins.exec, so every command that touches `packages` needs
      #   --option allow-unsafe-native-code-during-evaluation true
      packages = eachSystem (system: pkgs:
        let
          # This pkgs builds gonixgo's own tool and the Go program.
          goEnv = gonixgo.lib.mkGoEnv { inherit pkgs; };
        in {
          # The command, narrenhaus. The build also runs the tests of the
          # packages it is made of; those that need the nix program skip
          # themselves in the sandbox. It does not run the tests of e2e:
          # gonixgo tests the packages the command contains, and e2e is
          # test files alone. `go test ./e2e/` in the dev shell runs them.
          default = goEnv.buildGoApplication {
            pname = "narrenhaus";
            src = ./.;
            subPackages = [ "cmd/narrenhaus" ];
            meta.license = pkgs.lib.licenses.lgpl3Only;
          };
        });

      # The shell carries the built command, so entering it (or direnv
      # reloading it after a source change, see .envrc) rebuilds it. It
      # therefore needs the same evaluation option as `packages`.
      devShells = eachSystem (system: pkgs: {
        default = pkgs.mkShell {
          hardeningDisable = [ "all" ];

          packages = with pkgs; [ go self.packages.${system}.default ];
        };

        # Go without the built command. The default shell cannot be
        # entered while the module does not resolve, for example after a
        # `go get` that left go.sum behind; this one can, to run
        # `go mod tidy`. It needs no evaluation option.
        bare = pkgs.mkShell {
          hardeningDisable = [ "all" ];

          packages = with pkgs; [ go ];
        };
      });
    };
}
