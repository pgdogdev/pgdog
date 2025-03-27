use crate::{
    frontend::Buffer,
    net::messages::{Message, Protocol},
};

#[derive(Debug, Clone, Default, PartialEq, PartialOrd)]
pub struct Counter {
    row_description: i64,
    parameter_descripton: i64,
    ready_for_query: i64,
    command_complete: i64,
    in_transaction: bool,
    describe: bool,
}

impl Counter {
    pub fn count(&mut self, buffer: &Buffer) {
        for message in buffer.iter() {
            match message.code() {
                'D' => {
                    self.row_description += 1;
                    self.parameter_descripton += 1;
                    self.describe = true;
                }

                'Q' | 'E' => {
                    self.ready_for_query += 1;
                    self.command_complete += 1;
                }

                _ => (),
            }
        }
    }

    pub fn receive(&mut self, message: &Message) {
        match message.code() {
            'Z' => {
                self.ready_for_query -= 1;
                self.in_transaction = message.in_transaction();
            }

            'C' => {
                self.command_complete -= 1;
            }

            'E' => {
                self.command_complete -= 1;
                self.parameter_descripton -= 1;
                self.row_description -= 1;
            }

            'T' => {
                self.row_description -= 1;
            }

            't' => {
                self.parameter_descripton -= 1;
            }

            'n' => {
                self.row_description -= 1;
            }

            _ => (),
        }
    }

    pub fn done(&self) -> bool {
        self.row_description <= 0
            && self.command_complete <= 0
            && self.ready_for_query <= 0
            && self.parameter_descripton <= 0
            && !self.in_transaction
    }

    pub fn describe(&self) -> bool {
        self.describe
    }
}
