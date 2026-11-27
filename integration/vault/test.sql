\set ON_ERROR_STOP on

DO $$
DECLARE
    v_id bigint := floor(random() * 1000000 + 1)::bigint;
    v_count bigint;
BEGIN
    INSERT INTO tenants (id, name, billing_email)
    VALUES (v_id, 'tenant-' || v_id, 'billing-' || v_id || '@example.test')
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO accounts (id, tenant_id, email, full_name)
    VALUES (v_id, v_id, 'user-' || v_id || '@example.test', 'User ' || v_id)
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO projects (id, tenant_id, owner_account_id, name, status)
    VALUES (v_id, v_id, v_id, 'project-' || v_id, 'active')
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO tasks (id, tenant_id, project_id, assignee_account_id, title, state)
    VALUES (v_id, v_id, v_id, v_id, 'task-' || v_id, 'open')
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO task_comments (id, tenant_id, task_id, author_account_id, body)
    VALUES (v_id, v_id, v_id, v_id, 'comment body ' || v_id)
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO settings (id, name, value)
    VALUES (v_id, 'setting-' || v_id, 'value-' || v_id)
    ON CONFLICT (id) DO NOTHING;

    UPDATE tasks
    SET state = CASE WHEN state = 'open' THEN 'closed' ELSE 'open' END
    WHERE id = v_id;

    UPDATE accounts
    SET full_name = 'User ' || v_id || ' updated'
    WHERE id = v_id;

END;
$$;
