use crate::frontend::SetParam;

use super::*;

impl QueryEngine {
    pub(crate) async fn set(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        params: &[SetParam],
    ) -> Result<(), Error> {
        for param in params {
            if context.in_transaction() {
                context
                    .params
                    .insert_transaction(&param.name, param.value.clone(), param.local);
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
            self.fake_command_response(context, "SET").await?;
        }

        Ok(())
    }
}
