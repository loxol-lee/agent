from pathlib import Path


BASE_DIR = Path("/Users/lixiaoyi/Documents/newtext/agunt")

DIRS = [
    "apps/web-chat/public",
]

FILES = [
    "apps/web-chat/public/.keep",
]


def main() -> None:
    if not BASE_DIR.exists():
        raise SystemExit(f"Base dir does not exist: {BASE_DIR}")

    created_dirs = []
    for d in DIRS:
        p = BASE_DIR / d
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            created_dirs.append(str(p))

    created_files = []
    for f in FILES:
        p = BASE_DIR / f
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.touch(exist_ok=True)
            created_files.append(str(p))

    print(f"Base: {BASE_DIR}")
    print(f"Created dirs: {len(created_dirs)}")
    for x in created_dirs:
        print(f"- {x}")
    print(f"Created files: {len(created_files)}")
    for x in created_files:
        print(f"- {x}")


if __name__ == "__main__":
    main()