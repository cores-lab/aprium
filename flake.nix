{
  description = "prj";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = inputs @ { flake-parts, ... }:
  flake-parts.lib.mkFlake {inherit inputs;} {
    systems = [ "x86_64-linux" ];
    perSystem = { config, self', inputs', pkgs, system, ... }: {
      devShells.default = pkgs.mkShell {
        packages = builtins.attrValues { inherit (pkgs) gnumake gcc; };
      };
    };
  };

  nixConfig = { bash-prompt-suffix = "dev: "; };
}
