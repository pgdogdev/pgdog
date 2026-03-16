use crate::frontend::SetParam;

use super::*;

impl QueryEngine {
    pub(crate) async fn set(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        params: &[SetParam],
    ) -> Result<(), Error> {
        let mut fake_command = "SET";
        for param in params {
            if param.reset {
                context.params.reset(&param.name);
                fake_command = "RESET";
            } else if context.in_transaction() {
                context.params.insert_transaction(
                    &param.name,
                    param.value.clone(),
                    param.local,
                );
            } else {
                context.params.insert(&param.name, param.value.clone());
            }
        }

        if !context.in_transaction() {
            self.comms.update_params(context.params);
        }

        if self.backend.connected() {
            self.execute(context).await?;
        } else {
            self.fake_command_response(context, fake_command).await?;
        }

        Ok(())
    }

    pub(crate) async fn reset_all(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        context.params.reset_all();

        if self.backend.connected() {
            self.execute(context).await?;
        } else {
            self.fake_command_response(context, "RESET").await?;
        }

        Ok(())
    }
}
