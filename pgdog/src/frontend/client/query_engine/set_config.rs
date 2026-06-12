use crate::frontend::SetParam;

use super::*;

impl QueryEngine {
    pub(crate) async fn set_config(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        param: SetParam,
    ) -> Result<(), Error> {
        self.pending_set_config = Some(param);

        match self.execute(context).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.pending_set_config = None;
                Err(err)
            }
        }
    }

    pub(super) fn apply_pending_set_config(&mut self, context: &mut QueryEngineContext<'_>) {
        let Some(param) = self.pending_set_config.take() else {
            return;
        };

        if param.local {
            if context.in_transaction() {
                context
                    .params
                    .insert_transaction(&param.name, param.value, true);
            }
            return;
        }

        if context.in_transaction() {
            context
                .params
                .insert_transaction(&param.name, param.value, false);
        } else {
            context.params.insert(&param.name, param.value);
            self.comms.update_params(context.params);
        }
    }

    pub(super) fn clear_pending_set_config(&mut self) {
        self.pending_set_config = None;
    }
}
