use crate::net::parameter::ParameterValue;

use super::*;

impl QueryEngine {
    pub(crate) async fn set(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        name: String,
        value: ParameterValue,
        local: bool,
    ) -> Result<(), Error> {
        if context.in_transaction() {
            context
                .params
                .insert_transaction(name, value.clone(), local);
        } else {
            context.params.insert(name, value.clone());
            self.comms.update_params(context.params);
        }

        self.fake_command_response(context, "SET").await?;

        Ok(())
    }
}
