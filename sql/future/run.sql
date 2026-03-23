USE performance_monitoring;

-- ============================================================================
-- MYSQL CLI OR MYSQL SHELL RUNNER
-- ============================================================================
-- This file uses SOURCE, which is a mysql-client command (not SQL).
-- If you execute this in a GUI SQL editor tab, you'll get Error 1064.

SOURCE sql/schema.sql;
SOURCE sql/reset.sql;

SOURCE sql/views.sql;

SOURCE sql/kpi.sql;
SOURCE sql/dashboard.sql;

-- Optional operational automation layer:
SOURCE sql/future/ops.sql;
