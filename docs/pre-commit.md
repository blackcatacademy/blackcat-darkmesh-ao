# Pre-commit (optional)

Sample `.pre-commit-config.yaml`:
```
repos:
  - repo: https://github.com/JohnnyMorganz/StyLua
    rev: v0.20.0
    hooks:
      - id: stylua
        args: ["--config-path", ".stylua.toml"]
  - repo: https://github.com/mpeterv/luacheck
    rev: v1.2.0
    hooks:
      - id: luacheck
        additional_dependencies: []
```
Install: `pip install pre-commit` then `pre-commit install`.
