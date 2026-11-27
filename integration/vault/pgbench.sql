\set id random(1, 1000000)

INSERT INTO tenants (id, name, billing_email)
VALUES (:id, 'tenant-' || :id, 'billing-' || :id || '@example.test')
ON CONFLICT (id) DO NOTHING;

INSERT INTO accounts (id, tenant_id, email, full_name)
VALUES (:id, :id, 'user-' || :id || '@example.test', 'User ' || :id)
ON CONFLICT (id) DO NOTHING;

INSERT INTO projects (id, tenant_id, owner_account_id, name, status)
VALUES (:id, :id, :id, 'project-' || :id, 'active')
ON CONFLICT (id) DO NOTHING;

INSERT INTO tasks (id, tenant_id, project_id, assignee_account_id, title, state)
VALUES (:id, :id, :id, :id, 'task-' || :id, 'open')
ON CONFLICT (id) DO NOTHING;

INSERT INTO task_comments (id, tenant_id, task_id, author_account_id, body)
VALUES (:id, :id, :id, :id, 'comment body ' || :id)
ON CONFLICT (id) DO NOTHING;

INSERT INTO settings (id, name, value)
VALUES (:id, 'setting-' || :id, 'value-' || :id)
ON CONFLICT (id) DO NOTHING;

SELECT a.id, a.email, a.full_name, t.name AS tenant_name
FROM accounts a
JOIN tenants t ON t.id = a.tenant_id
WHERE a.id = :id;

SELECT p.id, p.name, p.status, a.email AS owner_email
FROM projects p
JOIN accounts a ON a.tenant_id = p.tenant_id AND a.id = p.owner_account_id
WHERE p.id = :id;

SELECT t.id, t.title, t.state, p.name AS project_name
FROM tasks t
JOIN projects p ON p.tenant_id = t.tenant_id AND p.id = t.project_id
WHERE t.id = :id;

SELECT tc.id, tc.body, a.full_name AS author
FROM task_comments tc
JOIN accounts a ON a.tenant_id = tc.tenant_id AND a.id = tc.author_account_id
WHERE tc.id = :id;

UPDATE tasks
SET state = CASE WHEN state = 'open' THEN 'closed' ELSE 'open' END
WHERE id = :id;

UPDATE accounts
SET full_name = 'User ' || :id || ' updated'
WHERE id = :id;

SELECT name, value FROM settings WHERE name = 'setting-' || :id;
