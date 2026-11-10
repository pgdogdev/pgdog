use crate::backend::pool::lb::{Candidate, primary_reads};
use crate::config::{ReadWriteSplit, Role};

#[derive(Debug)]
struct TestTarget {
    role: Role,
    banned: bool,
}

impl Candidate for TestTarget {
    fn role(&self) -> Role {
        self.role
    }

    fn banned(&self) -> bool {
        self.banned
    }
}

const P: TestTarget = TestTarget {
    role: Role::Primary,
    banned: false,
};
const P_BANNED: TestTarget = TestTarget {
    role: Role::Primary,
    banned: true,
};
const R: TestTarget = TestTarget {
    role: Role::Replica,
    banned: false,
};
const R_BANNED: TestTarget = TestTarget {
    role: Role::Replica,
    banned: true,
};

static HEALTHY: &[TestTarget] = &[R, R, P];
static ONE_REPLICA_BANNED: &[TestTarget] = &[R_BANNED, R, P];
static ALL_REPLICAS_BANNED: &[TestTarget] = &[R_BANNED, R_BANNED, P];
static PRIMARY_BANNED: &[TestTarget] = &[R, R, P_BANNED];
static ONE_REPLICA_FILTERED: &[TestTarget] = &[R, P];
static PRIMARY_ONLY: &[TestTarget] = &[P];
static REPLICAS_ONLY: &[TestTarget] = &[R, R];
static REPLICAS_ONLY_ALL_BANNED: &[TestTarget] = &[R_BANNED, R_BANNED];

struct Expected {
    include_primary: bool,
    include_primary_if_replica_lag: bool,
    include_primary_if_replica_banned: bool,
    exclude_primary: bool,
    prefer_primary: bool,
}

impl Expected {
    fn by_split(&self) -> [(ReadWriteSplit, bool); 5] {
        [
            (ReadWriteSplit::IncludePrimary, self.include_primary),
            (
                ReadWriteSplit::IncludePrimaryIfReplicaLag,
                self.include_primary_if_replica_lag,
            ),
            (
                ReadWriteSplit::IncludePrimaryIfReplicaBanned,
                self.include_primary_if_replica_banned,
            ),
            (ReadWriteSplit::ExcludePrimary, self.exclude_primary),
            (ReadWriteSplit::PreferPrimary, self.prefer_primary),
        ]
    }
}

struct Case {
    name: &'static str,
    candidates: &'static [TestTarget],
    read_only: bool,
    expected: Expected,
}

const CASES: &[Case] = &[
    Case {
        name: "healthy",
        candidates: HEALTHY,
        read_only: false,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: false,
            include_primary_if_replica_banned: false,
            exclude_primary: false,
            prefer_primary: false,
        },
    },
    Case {
        name: "healthy",
        candidates: HEALTHY,
        read_only: true,
        expected: Expected {
            include_primary: false,
            include_primary_if_replica_lag: false,
            include_primary_if_replica_banned: false,
            exclude_primary: false,
            prefer_primary: false,
        },
    },
    Case {
        name: "one replica banned",
        candidates: ONE_REPLICA_BANNED,
        read_only: false,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: false,
            include_primary_if_replica_banned: true,
            exclude_primary: false,
            prefer_primary: false,
        },
    },
    Case {
        name: "one replica banned",
        candidates: ONE_REPLICA_BANNED,
        read_only: true,
        expected: Expected {
            include_primary: false,
            include_primary_if_replica_lag: false,
            include_primary_if_replica_banned: false,
            exclude_primary: false,
            prefer_primary: false,
        },
    },
    Case {
        name: "all replicas banned",
        candidates: ALL_REPLICAS_BANNED,
        read_only: false,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: true,
            include_primary_if_replica_banned: true,
            exclude_primary: false,
            prefer_primary: true,
        },
    },
    Case {
        name: "all replicas banned",
        candidates: ALL_REPLICAS_BANNED,
        read_only: true,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: true,
            include_primary_if_replica_banned: true,
            exclude_primary: false,
            prefer_primary: true,
        },
    },
    Case {
        name: "primary banned",
        candidates: PRIMARY_BANNED,
        read_only: false,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: false,
            include_primary_if_replica_banned: false,
            exclude_primary: false,
            prefer_primary: false,
        },
    },
    Case {
        name: "primary banned",
        candidates: PRIMARY_BANNED,
        read_only: true,
        expected: Expected {
            include_primary: false,
            include_primary_if_replica_lag: false,
            include_primary_if_replica_banned: false,
            exclude_primary: false,
            prefer_primary: false,
        },
    },
    Case {
        name: "one replica filtered by lsn",
        candidates: ONE_REPLICA_FILTERED,
        read_only: false,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: false,
            include_primary_if_replica_banned: false,
            exclude_primary: false,
            prefer_primary: false,
        },
    },
    Case {
        name: "one replica filtered by lsn",
        candidates: ONE_REPLICA_FILTERED,
        read_only: true,
        expected: Expected {
            include_primary: false,
            include_primary_if_replica_lag: false,
            include_primary_if_replica_banned: false,
            exclude_primary: false,
            prefer_primary: false,
        },
    },
    Case {
        name: "primary only",
        candidates: PRIMARY_ONLY,
        read_only: false,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: true,
            include_primary_if_replica_banned: true,
            exclude_primary: true,
            prefer_primary: true,
        },
    },
    Case {
        name: "primary only",
        candidates: PRIMARY_ONLY,
        read_only: true,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: true,
            include_primary_if_replica_banned: true,
            exclude_primary: true,
            prefer_primary: true,
        },
    },
    Case {
        name: "replicas only",
        candidates: REPLICAS_ONLY,
        read_only: false,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: false,
            include_primary_if_replica_banned: false,
            exclude_primary: false,
            prefer_primary: false,
        },
    },
    Case {
        name: "replicas only",
        candidates: REPLICAS_ONLY,
        read_only: true,
        expected: Expected {
            include_primary: false,
            include_primary_if_replica_lag: false,
            include_primary_if_replica_banned: false,
            exclude_primary: false,
            prefer_primary: false,
        },
    },
    Case {
        name: "replicas only, all banned",
        candidates: REPLICAS_ONLY_ALL_BANNED,
        read_only: false,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: true,
            include_primary_if_replica_banned: true,
            exclude_primary: false,
            prefer_primary: true,
        },
    },
    Case {
        name: "replicas only, all banned",
        candidates: REPLICAS_ONLY_ALL_BANNED,
        read_only: true,
        expected: Expected {
            include_primary: true,
            include_primary_if_replica_lag: true,
            include_primary_if_replica_banned: true,
            exclude_primary: false,
            prefer_primary: true,
        },
    },
];

#[test]
fn test_primary_reads_matrix() {
    let mut failures = vec![];

    for case in CASES {
        for (split, expected) in case.expected.by_split() {
            let actual = primary_reads(split, case.read_only, case.candidates);
            if actual != expected {
                failures.push(format!(
                    "{} (read_only = {}), {}: expected {}, got {}",
                    case.name, case.read_only, split, expected, actual
                ));
            }
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
