use super::*;

impl QueryParser {
    /// Handle the SET command.
    ///
    /// We allow setting shard/sharding key manually outside
    /// the normal protocol flow. This command is not forwarded to the server.
    ///
    /// All other SETs change the params on the client and are eventually sent to the server
    /// when the client is connected to the server.
    pub(super) fn set(
        &mut self,
        stmt: &VariableSetStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        match stmt.name.as_str() {
            "pgdog.shard" => {
                if self.in_transaction {
                    let node = stmt
                        .args
                        .first()
                        .ok_or(Error::SetShard)?
                        .node
                        .as_ref()
                        .ok_or(Error::SetShard)?;
                    if let NodeEnum::AConst(AConst {
                        val: Some(a_const::Val::Ival(Integer { ival })),
                        ..
                    }) = node
                    {
                        return Ok(Command::Query(
                            Route::write(Some(*ival as usize)).set_read(context.read_only),
                        ));
                    }
                } else {
                    return Err(Error::RequiresTransaction);
                }
            }

            "pgdog.sharding_key" => {
                if self.in_transaction {
                    let node = stmt
                        .args
                        .first()
                        .ok_or(Error::SetShard)?
                        .node
                        .as_ref()
                        .ok_or(Error::SetShard)?;

                    if let NodeEnum::AConst(AConst {
                        val: Some(Val::Sval(String { sval })),
                        ..
                    }) = node
                    {
                        let shard = if context.sharding_schema.shards > 1 {
                            let ctx = ContextBuilder::infer_from_from_and_config(
                                sval.as_str(),
                                &context.sharding_schema,
                            )?
                            .shards(context.shards)
                            .build()?;
                            ctx.apply()?
                        } else {
                            Shard::Direct(0)
                        };
                        return Ok(Command::Query(
                            Route::write(shard).set_read(context.read_only),
                        ));
                    }
                } else {
                    return Err(Error::RequiresTransaction);
                }
            }

            // TODO: Handle SET commands for updating client
            // params without touching the server.
            name => {
                if !self.in_transaction {
                    let mut value = vec![];

                    for node in &stmt.args {
                        if let Some(NodeEnum::AConst(AConst { val: Some(val), .. })) = &node.node {
                            match val {
                                Val::Sval(String { sval }) => {
                                    value.push(sval.to_string());
                                }

                                Val::Ival(Integer { ival }) => {
                                    value.push(ival.to_string());
                                }

                                Val::Fval(Float { fval }) => {
                                    value.push(fval.to_string());
                                }

                                Val::Boolval(Boolean { boolval }) => {
                                    value.push(boolval.to_string());
                                }

                                _ => (),
                            }
                        }
                    }

                    match value.len() {
                        0 => (),
                        1 => {
                            return Ok(Command::Set {
                                name: name.to_string(),
                                value: ParameterValue::String(value.pop().unwrap()),
                            })
                        }
                        _ => {
                            return Ok(Command::Set {
                                name: name.to_string(),
                                value: ParameterValue::Tuple(value),
                            })
                        }
                    }
                }
            }
        }

        Ok(Command::Query(
            Route::write(Shard::All).set_read(context.read_only),
        ))
    }
}
