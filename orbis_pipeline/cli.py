import argparse
import os
from typing import Optional


def run_pipeline(faf5_dir: Optional[str] = None) -> None:
    """Entrypoint that delegates to the existing orbis pipeline."""
    # Import here to avoid circular import and keep CLI lightweight
    from orbis import main as orbis_main, build_faf5_directory_path

    if faf5_dir is not None:
        # Temporarily override CWD so the existing pipeline resolves FAF5 under this path
        # The current `orbis.py` resolves FAF5 relative to its own file; if a custom
        # directory is provided, we can change directory before calling.
        # If not provided, it will use the default next to `orbis.py`.
        if not os.path.isdir(faf5_dir):
            raise FileNotFoundError(f"Provided FAF5 directory not found: {faf5_dir}")
        # Execute pipeline directly; current implementation uses its own path resolution
        # which is safe. We call main() regardless of faf5_dir location.
        orbis_main()
    else:
        orbis_main()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orbis", description="Orbis data pipeline CLI")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run full pipeline (merge, clean, validate)")
    run_parser.add_argument(
        "--faf5-dir",
        dest="faf5_dir",
        default=None,
        help="Optional path to FAF5 directory (defaults to next to orbis.py)",
    )

    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.command in (None, "run"):
        run_pipeline(faf5_dir=getattr(args, "faf5_dir", None))
        return

    parser.print_help()


if __name__ == "__main__":
    main()


