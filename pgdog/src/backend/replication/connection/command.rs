#[derive(Debug, Clone)]
pub enum Command {
    CreateReplicationSlot { name: String, temporary: bool },
    Checkpoint,
}

impl Command {
    fn to_sql(&self) -> String {
        match self {
            Command::CreateReplicationSlot { name, temporary } => {
                format!(
                    "CREATE_REPLICATION_SLOT {}{} LOGICAL 'pgoutput'",
                    name,
                    if *temporary { " TEMPORARY" } else { "" }
                )
            }
            Command::Checkpoint => format!("CHECKPOINT"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_create_replication_slot() {
        let cmd = Command::CreateReplicationSlot {
            name: "test".into(),
            temporary: true,
        };
        assert_eq!(
            cmd.to_sql(),
            "CREATE_REPLICATION_SLOT test TEMPORARY LOGICAL 'pgoutput'"
        );
    }
}
