\set id_seed random(1, 1000000000)

INSERT INTO tenants (id, name, billing_email)
VALUES (
    :id_seed,
    'tenant-' || :id_seed || '-copy',
    'tenant-' || :id_seed || '-copy@example.test'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO accounts (id, tenant_id, email, full_name)
VALUES (
    :id_seed,
    :id_seed,
    'account-' || :id_seed || '-copy@example.test',
    'account-' || :id_seed || '-copy'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO projects (id, tenant_id, owner_account_id, name, status)
VALUES (
    :id_seed,
    :id_seed,
    :id_seed,
    'project-' || :id_seed || '-copy',
    'active'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO tasks (id, tenant_id, project_id, assignee_account_id, title, state)
VALUES (
    :id_seed,
    :id_seed,
    :id_seed,
    :id_seed,
    'task-' || :id_seed || '-copy',
    'open'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO task_comments (id, tenant_id, task_id, author_account_id, body)
VALUES (
    :id_seed,
    :id_seed,
    :id_seed,
    :id_seed,
    'comment-' || :id_seed || '-copy'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO settings (id, name, value)
VALUES (
    :id_seed,
    'setting-' || :id_seed || '-copy',
    'value-' || :id_seed || '-copy'
)
ON CONFLICT (id) DO NOTHING;

UPDATE tenants
SET name = 'tenant-' || :id_seed || '-replicate'
WHERE id = :id_seed;

UPDATE accounts
SET full_name = 'account-' || :id_seed || '-replicate'
WHERE id = :id_seed;

UPDATE projects
SET name = 'project-' || :id_seed || '-replicate'
WHERE id = :id_seed;

UPDATE tasks
SET title = 'task-' || :id_seed || '-replicate'
WHERE id = :id_seed;

UPDATE task_comments
SET body = 'comment-' || :id_seed || '-replicate'
WHERE id = :id_seed;

UPDATE settings
SET name = 'setting-' || :id_seed || '-replicate'
WHERE id = :id_seed;
