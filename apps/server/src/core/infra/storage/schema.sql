PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS traces (
  trace_id TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER,
  status TEXT NOT NULL,
  stop_reason TEXT,
  error_message TEXT
);

CREATE TABLE IF NOT EXISTS spans (
  span_id TEXT PRIMARY KEY,
  trace_id TEXT NOT NULL,
  name TEXT NOT NULL,
  kind TEXT NOT NULL,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER,
  ok INTEGER NOT NULL,
  attrs_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_spans_trace_id ON spans(trace_id);

CREATE TABLE IF NOT EXISTS conversations (
  conversation_id TEXT PRIMARY KEY,
  title TEXT,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  archived INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_conversations_updated_at_ms ON conversations(updated_at_ms);
CREATE INDEX IF NOT EXISTS idx_conversations_archived_updated_at_ms ON conversations(archived, updated_at_ms);

CREATE TABLE IF NOT EXISTS conversation_settings (
  conversation_id TEXT PRIMARY KEY,
  memory_write_enabled INTEGER NOT NULL DEFAULT 1,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS memories (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  conversation_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  content TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_memories_conversation_id_created_at_ms ON memories(conversation_id, created_at_ms);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  conversation_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_trace_id ON messages(trace_id);

CREATE TABLE IF NOT EXISTS replay_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trace_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_replay_trace_id_seq ON replay_events(trace_id, seq);

CREATE TABLE IF NOT EXISTS tool_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trace_id TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  call_id TEXT NOT NULL,
  tool_name TEXT NOT NULL,
  allowed INTEGER NOT NULL,
  risk TEXT NOT NULL,
  side_effect INTEGER NOT NULL,
  args_json TEXT NOT NULL,
  result_json TEXT NOT NULL,
  ok INTEGER NOT NULL,
  error_code TEXT,
  error_message TEXT,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER NOT NULL,
  latency_ms INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  UNIQUE(trace_id, call_id)
);

CREATE INDEX IF NOT EXISTS idx_tool_runs_trace_id ON tool_runs(trace_id);
CREATE INDEX IF NOT EXISTS idx_tool_runs_conversation_id_created_at_ms ON tool_runs(conversation_id, created_at_ms);

CREATE TABLE IF NOT EXISTS agent_tasks (
  task_id TEXT PRIMARY KEY,
  goal TEXT NOT NULL,
  status TEXT NOT NULL,
  current_step TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  max_retry INTEGER NOT NULL,
  acceptance_cmd TEXT NOT NULL,
  working_dir TEXT NOT NULL,
  dry_run INTEGER NOT NULL DEFAULT 0,
  rollback_on_action_failure INTEGER NOT NULL DEFAULT 1,
  actor_role TEXT NOT NULL DEFAULT 'coder_agent',
  request_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  stop_reason TEXT,
  error_code TEXT,
  scope_paths_json TEXT NOT NULL,
  forbidden_paths_json TEXT NOT NULL,
  actions_json TEXT NOT NULL,
  last_verify_json TEXT,
  logs_json TEXT,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_agent_tasks_created_at_ms ON agent_tasks(created_at_ms);
CREATE INDEX IF NOT EXISTS idx_agent_tasks_status_updated_at_ms ON agent_tasks(status, updated_at_ms);

CREATE TABLE IF NOT EXISTS agent_task_artifacts (
  task_id TEXT PRIMARY KEY,
  changed_files_json TEXT NOT NULL,
  step_results_json TEXT NOT NULL,
  test_result TEXT,
  summary TEXT,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_task_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  ts INTEGER NOT NULL,
  message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_task_logs_task_id_id ON agent_task_logs(task_id, id);

CREATE TABLE IF NOT EXISTS agent_task_steps (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  step_no INTEGER NOT NULL,
  step_type TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  input_json TEXT NOT NULL,
  output_json TEXT NOT NULL,
  ok INTEGER NOT NULL,
  latency_ms INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_task_steps_task_id_id ON agent_task_steps(task_id, id);

CREATE TABLE IF NOT EXISTS group_tasks (
  task_id TEXT PRIMARY KEY,
  goal TEXT NOT NULL,
  status TEXT NOT NULL,
  phase TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  stop_reason TEXT,
  error_code TEXT,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_group_tasks_created_at_ms ON group_tasks(created_at_ms);
CREATE INDEX IF NOT EXISTS idx_group_tasks_status_updated_at_ms ON group_tasks(status, updated_at_ms);

CREATE TABLE IF NOT EXISTS group_task_rounds (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  round_no INTEGER NOT NULL,
  phase TEXT NOT NULL,
  actor_role TEXT NOT NULL,
  note TEXT,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_group_task_rounds_task_id_id ON group_task_rounds(task_id, id);

CREATE TABLE IF NOT EXISTS group_agent_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  round_no INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  refs_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_group_agent_messages_task_id_id ON group_agent_messages(task_id, id);

CREATE TABLE IF NOT EXISTS group_artifacts (
  task_id TEXT PRIMARY KEY,
  changed_files_json TEXT NOT NULL,
  test_result TEXT,
  summary TEXT,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS group_decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  decision_type TEXT NOT NULL,
  winner_plan_id TEXT,
  reason TEXT,
  approved_by TEXT,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_group_decisions_task_id_id ON group_decisions(task_id, id);
