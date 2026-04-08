CREATE TABLE tenants (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    billing_email TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, email),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id)
);

CREATE TABLE projects (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    owner_account_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id)
);

CREATE TABLE tasks (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    assignee_account_id BIGINT,
    title TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'open',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id),
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects (tenant_id, id),
    FOREIGN KEY (tenant_id, assignee_account_id) REFERENCES accounts (tenant_id, id)
);

CREATE TABLE task_comments (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    task_id BIGINT NOT NULL,
    author_account_id BIGINT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id),
    FOREIGN KEY (tenant_id, task_id) REFERENCES tasks (tenant_id, id),
    FOREIGN KEY (tenant_id, author_account_id) REFERENCES accounts (tenant_id, id)
);

CREATE TABLE settings (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_accounts_tenant_id ON accounts (tenant_id);
CREATE INDEX idx_projects_tenant_id ON projects (tenant_id);
CREATE INDEX idx_projects_owner ON projects (tenant_id, owner_account_id);
CREATE INDEX idx_tasks_tenant_id ON tasks (tenant_id);
CREATE INDEX idx_tasks_project ON tasks (tenant_id, project_id);
CREATE INDEX idx_tasks_assignee ON tasks (tenant_id, assignee_account_id);
CREATE INDEX idx_task_comments_tenant_id ON task_comments (tenant_id);
CREATE INDEX idx_task_comments_task ON task_comments (tenant_id, task_id);

CREATE PUBLICATION pgdog FOR ALL TABLES;
