USE performance_monitoring;

-- ============================================================================
-- MYSQL CLI OR MYSQL SHELL RUNNER
-- ============================================================================


SOURCE sql/schema.sql;
SOURCE sql/reset.sql;

SOURCE sql/views.sql;
SOURCE sql/kpi.sql;
SOURCE sql/dashboard.sql;

-- Optional operational automation layer:
SOURCE sql/future/ops.sql;
