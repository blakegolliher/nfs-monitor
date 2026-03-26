# Contributing

Thanks for your interest in contributing to nfs-monitor.

## Prerequisites

- Go 1.21 or later
- A Linux system with NFS mounts (for runtime testing)

## Getting started

```bash
git clone <repo-url>
cd nfs-monitor
go build -o nfs-monitor .
```

## Project structure

The project is a single Go binary with no external dependencies (standard library only).

- `main.go` -- all source code: monitoring, reporting, and the compare subcommand
- `go.mod` -- module definition

## Making changes

1. Fork the repository and create a feature branch.
2. Make your changes in `main.go`.
3. Ensure the code compiles and passes vet:
   ```bash
   go build ./...
   go vet ./...
   ```
4. Test on a system with NFS mounts if your change affects monitoring or parsing logic.
5. Open a pull request with a clear description of what changed and why.

## Guidelines

- Keep external dependencies at zero -- standard library only.
- Match the existing code style. Run `gofmt` before committing.
- The output format is parsed by the `compare` subcommand. If you change report formatting, update the compare parser regexes to match.
- Test the compare subcommand against captured output files when changing report formatting.

## Reporting issues

Open an issue describing the problem, including:
- The NFS version and mount options in use
- The output or error you received
- What you expected to happen
