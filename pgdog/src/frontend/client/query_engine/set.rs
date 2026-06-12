use crate::frontend::SetParam;

use super::*;

impl QueryEngine {
    pub(crate) async fn set(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        params: &[SetParam],
        behave_like_select: bool,
    ) -> Result<(), Error> {
        let mut fake_command = "SET";
        for param in params {
            if let Some(value) = param.value.clone() {
                if context.in_transaction() {
                    context
                        .params
                        .insert_transaction(&param.name, value, param.local);
                } else {
                    context.params.insert(&param.name, value);
                }
            } else {
                fake_command = "RESET";
                context.params.reset(&param.name);
            }
        }

        if !context.in_transaction() {
            self.comms.update_params(context.params);
        }

        if self.backend.connected() {
            self.execute(context).await?;
        } else {
            let values_to_return =
                behave_like_select.then(|| params.iter().map(|p| p.value.as_ref()));
            self.fake_command_response(context, fake_command, values_to_return)
                .await?;
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
            self.fake_command_response(context, "RESET", None::<Option<_>>)
                .await?;
        }

        Ok(())
    }
}
