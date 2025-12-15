# Pixi initialization

The following describes the initialization of the pixi environment using these
shell commands:

```
pixi init
pixi add --feature dev "pytest=*" "ruff=*" "hatchling=*"
pixi workspace environment add production --solve-group prod
pixi workspace environment add default --feature dev --solve-group prod --force
```

This initialization sets up the project workspace so that there exists two environments, the "default" environment for development, which has specific development tools (e.g., pytest, ruff, hatchling), and a "production" environment that mimicks a Pypi installation defined by a pyproject.toml configuration file.

1. `pixi init`: initialize the pixi environment by creating a pixi.toml.
2. `pixi add...`: create a feature containing the dependencies `pytest`, `ruff`, and `hatchling`; the `=*` allows flexibility for multiple versions.
3. `pixi workspace environment...`: add an environment to the project workspace; `--solve-group prod` makes sure that the dependencies are solved as if they were in the same environment; `--feature dev` adds the "dev" feature dependencies only to the default environment; `--force` to overwrite the "default" environment.