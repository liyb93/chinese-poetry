PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;

CREATE TABLE IF NOT EXISTS authors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  dynasty TEXT,
  desc TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_authors_name ON authors(name);

CREATE TABLE IF NOT EXISTS poems (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT,
  author TEXT,
  dynasty TEXT,
  genre TEXT,            -- tang/song/ci/other
  source_file TEXT,      -- 源 JSON 文件相对路径
  paragraphs_json TEXT,  -- 保留段落原始数组(JSON)
  content TEXT,          -- 连接后的纯文本（\n 拼接）
  tags_json TEXT,        -- 若存在 tags 字段
  extra_json TEXT        -- 其他字段打包
);

CREATE INDEX IF NOT EXISTS idx_poems_author ON poems(author);
CREATE INDEX IF NOT EXISTS idx_poems_title ON poems(title);
CREATE INDEX IF NOT EXISTS idx_poems_genre ON poems(genre);