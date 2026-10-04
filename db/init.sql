-- ByteArena database schema. Runs once, automatically, on first Postgres start
-- (mounted into /docker-entrypoint-initdb.d by docker-compose.yml).
-- Money-free, ORM-free: plain SQL on purpose.

CREATE TABLE problems (
  id               text PRIMARY KEY,
  title            text    NOT NULL,
  statement        text    NOT NULL,
  time_limit_ms    integer NOT NULL CHECK (time_limit_ms BETWEEN 100 AND 10000),
  memory_limit_mb  integer NOT NULL CHECK (memory_limit_mb BETWEEN 64 AND 512)
);

CREATE TABLE test_cases (
  problem_id       text    NOT NULL REFERENCES problems(id) ON DELETE CASCADE,
  test_index       integer NOT NULL,          -- 1-based, defines judging order
  input            text    NOT NULL,
  expected_output  text    NOT NULL,
  is_sample        boolean NOT NULL DEFAULT false,  -- only samples may ever be shown to users
  PRIMARY KEY (problem_id, test_index)
);

CREATE TABLE submissions (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  problem_id       text NOT NULL REFERENCES problems(id),
  language         text NOT NULL CHECK (language IN ('PYTHON', 'JAVASCRIPT')),
  handle           text NOT NULL CHECK (handle ~ '^[a-zA-Z0-9_-]{1,32}$'),
  code             text NOT NULL CHECK (char_length(code) BETWEEN 1 AND 65536),
  status           text NOT NULL DEFAULT 'QUEUED'
                   CHECK (status IN ('QUEUED', 'JUDGING', 'COMPLETED', 'SYSTEM_ERROR')),
  verdict          text NULL
                   CHECK (verdict IN ('ACCEPTED', 'WRONG_ANSWER', 'RUNTIME_ERROR',
                                      'TIME_LIMIT_EXCEEDED', 'MEMORY_LIMIT_EXCEEDED',
                                      'OUTPUT_LIMIT_EXCEEDED', 'INTERNAL_ERROR')),
  idempotency_key  text NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  judged_at        timestamptz NULL
);

-- Idempotent submit. Partial unique index: use in SQL as
--   INSERT ... ON CONFLICT (handle, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
CREATE UNIQUE INDEX submissions_idempotency_uq
  ON submissions (handle, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX submissions_handle_created_idx ON submissions (handle, created_at DESC);

-- One row per (submission, test). The primary key makes result writing idempotent:
--   INSERT ... ON CONFLICT (submission_id, test_index) DO NOTHING
CREATE TABLE test_results (
  submission_id    uuid    NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
  test_index       integer NOT NULL,
  verdict          text    NOT NULL,
  time_ms          integer NOT NULL DEFAULT 0,
  memory_kb        integer NOT NULL DEFAULT 0,
  is_sample        boolean NOT NULL DEFAULT false,
  created_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (submission_id, test_index)
);

-- Transactional outbox. Rows are inserted in the SAME transaction as the state change
-- (see CreateSubmission) and drained by the outbox-publisher.
CREATE TABLE outbox (
  id            bigserial PRIMARY KEY,        -- publish order
  topic         text  NOT NULL,
  msg_key       text  NOT NULL,               -- Kafka message key (submission id)
  payload       jsonb NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  published_at  timestamptz NULL
);

-- The publisher only ever scans unpublished rows, so keep that index tiny.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- ---------------------------------------------------------------------------
-- Seed data: 3 problems. Test 1 and 2 are samples (visible), the rest are hidden.
-- ---------------------------------------------------------------------------

INSERT INTO problems (id, title, statement, time_limit_ms, memory_limit_mb) VALUES
('sum-two', 'Sum of Two Numbers',
 'Read two integers a and b from one line of standard input, separated by a space. Print a + b.',
 2000, 128),
('reverse-string', 'Reverse a String',
 'Read one line containing a string (letters and spaces, no leading or trailing spaces). Print it reversed.',
 2000, 128),
('max-of-array', 'Maximum of an Array',
 'The first line holds n. The second line holds n space-separated integers. Print the largest one.',
 2000, 128);

INSERT INTO test_cases (problem_id, test_index, input, expected_output, is_sample) VALUES
('sum-two', 1, E'1 2\n',                         E'3\n',          true),
('sum-two', 2, E'-5 5\n',                        E'0\n',          true),
('sum-two', 3, E'1000000000 1000000000\n',       E'2000000000\n', false),
('sum-two', 4, E'0 0\n',                         E'0\n',          false),
('sum-two', 5, E'123456 654321\n',               E'777777\n',     false),

('reverse-string', 1, E'hello\n',                E'olleh\n',      true),
('reverse-string', 2, E'abc def\n',              E'fed cba\n',    true),
('reverse-string', 3, E'a\n',                    E'a\n',          false),
('reverse-string', 4, E'racecar\n',              E'racecar\n',    false),
('reverse-string', 5, E'ByteArena\n',            E'anerAetyB\n',  false),

('max-of-array', 1, E'5\n3 9 2 7 1\n',           E'9\n',          true),
('max-of-array', 2, E'3\n-1 -2 -3\n',            E'-1\n',         true),
('max-of-array', 3, E'1\n-4\n',                  E'-4\n',         false),
('max-of-array', 4, E'4\n1000000000 999999999 1000000000 5\n', E'1000000000\n', false),
('max-of-array', 5, E'6\n0 0 0 0 0 0\n',         E'0\n',          false);
