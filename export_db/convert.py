#!/usr/bin/env python3
import argparse, json, os, sqlite3, sys
from pathlib import Path

# 可能用到的源目录名（按 chinese-poetry 仓库常见结构）
CANDIDATE_DIRS = [
    "json",           # 新结构可能把多类数据放在 json/ 下
    "ci", "ci/ci.song", "ci/ci.song.1000", "ci/ci.song.200k",
    "poet", "poet/poet.tang", "poet/poet.song",
    "shi", "shi/shi.tang", "shi/shi.song",
    "tang", "song",
]

def find_data_dirs(root: Path):
    dirs = []
    for name in CANDIDATE_DIRS:
        p = root / name
        if p.exists():
            dirs.append(p)
    # 兜底：遍历所有包含 .json 的目录
    if not dirs:
        for p in root.rglob("*.json"):
            dirs.append(p.parent)
        dirs = sorted(set(dirs))
    return dirs

def ensure_schema(conn: sqlite3.Connection, schema_path: Path):
    with open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())

def normalize_author_name(v):
    if v is None: return None
    if isinstance(v, str): return v.strip()
    return str(v)

def extract_text(paragraphs):
    if isinstance(paragraphs, list):
        # 段落数组 -> 用换行拼接
        return "\n".join([str(x) for x in paragraphs])
    if isinstance(paragraphs, str):
        return paragraphs
    return None

def detect_genre_from_path(path: Path):
    parts = [p.lower() for p in path.parts]
    if "tang" in parts: return "tang"
    if "song" in parts and "ci" in parts: return "ci"
    if "ci" in parts: return "ci"
    if "song" in parts: return "song"
    return "other"

def insert_poem(cur, rec, source_file, genre):
    # 常见字段：title, author, paragraphs, dynasty, tags
    title = rec.get("title")
    author = normalize_author_name(rec.get("author"))
    dynasty = rec.get("dynasty")
    paragraphs = rec.get("paragraphs") or rec.get("paragraph") or rec.get("content")
    content = extract_text(paragraphs)
    tags = rec.get("tags")
    # 额外字段打包
    extra = {k: v for k, v in rec.items() if k not in {"title","author","paragraphs","paragraph","content","dynasty","tags"}}
    cur.execute(
        """INSERT INTO poems(title, author, dynasty, genre, source_file, paragraphs_json, content, tags_json, extra_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            title, author, dynasty, genre, source_file,
            json.dumps(paragraphs, ensure_ascii=False) if paragraphs is not None else None,
            content,
            json.dumps(tags, ensure_ascii=False) if tags is not None else None,
            json.dumps(extra, ensure_ascii=False) if extra else None
        )
    )

def try_load_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] skip {path}: {e}", file=sys.stderr)
        return None

def import_dir(conn, data_dir: Path, root: Path):
    cur = conn.cursor()
    files = list(data_dir.rglob("*.json"))
    for fp in files:
        data = try_load_json(fp)
        if data is None:
            continue
        genre = detect_genre_from_path(fp)
        rel = str(fp.relative_to(root))
        # 数据既可能是数组，也可能是对象里嵌套数组
        if isinstance(data, list):
            for rec in data:
                if isinstance(rec, dict):
                    insert_poem(cur, rec, rel, genre)
        elif isinstance(data, dict):
            # 一些 author 列表或嵌套结构
            # 尝试常见键
            for key in ("poems", "data", "items", "list"):
                arr = data.get(key)
                if isinstance(arr, list):
                    for rec in arr:
                        if isinstance(rec, dict):
                            insert_poem(cur, rec, rel, genre)
            # 也可能是作者信息
            name = data.get("name") or data.get("author")
            desc = data.get("desc") or data.get("description")
            dynasty = data.get("dynasty")
            if name and (desc or dynasty):
                cur.execute(
                    "INSERT OR IGNORE INTO authors(name, dynasty, desc) VALUES (?, ?, ?)",
                    (normalize_author_name(name), dynasty, desc)
                )
        conn.commit()

def main():
    ap = argparse.ArgumentParser(description="Convert chinese-poetry JSON to SQLite (no Go).")
    ap.add_argument("--source", required=True, help="Path to cloned chinese-poetry repo")
    ap.add_argument("--db", default="poems.sqlite3", help="Output sqlite db path")
    ap.add_argument("--schema", default="schema.sql", help="schema.sql path")
    args = ap.parse_args()

    source = Path(args.source).resolve()
    db_path = Path(args.db).resolve()
    schema_path = Path(args.schema).resolve()

    if not source.exists():
        print(f"source not found: {source}", file=sys.stderr)
        sys.exit(2)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_schema(conn, schema_path)

    data_dirs = find_data_dirs(source)
    if not data_dirs:
        print("No data dirs with json found.", file=sys.stderr)
        sys.exit(3)

    print(f"[INFO] Found {len(data_dirs)} data dirs")
    for d in data_dirs:
        print(f"[INFO] Importing {d}")
        import_dir(conn, d, source)

    # 简单的作者回填（从 poems 表聚合作者/朝代）
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO authors(name, dynasty, desc)
        SELECT author, dynasty, NULL
        FROM poems
        WHERE author IS NOT NULL AND author <> ''
        GROUP BY author, dynasty
    """)
    conn.commit()
    conn.close()
    print(f"[DONE] Wrote {db_path}")

if __name__ == "__main__":
    main()