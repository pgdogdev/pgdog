use std::ops::{Deref, DerefMut};

use crate::frontend::{
    ClientRequest,
    router::{Ast, Route},
};

#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum Error {
    #[error("only initial requests can be parsed")]
    InitialToParsed,

    #[error("request was not parsed")]
    NoAst,

    #[error("requested was not routed yet")]
    NoRoute,
}

pub(crate) enum EngineRequest<'a> {
    Initial(&'a mut ClientRequest),
    Parsed(ParsedEngineRequest<'a>),
    Routed(RoutedEngineRequest<'a>),
    Transitioning,
}

impl EngineRequest<'_> {
    pub(crate) fn into_parsed(&mut self, ast: Option<Ast>) -> Result<(), Error> {
        let request = match std::mem::replace(self, Self::Transitioning) {
            Self::Initial(request) => request,
            request => {
                *self = request;
                return Err(Error::InitialToParsed);
            }
        };

        *self = Self::Parsed(ParsedEngineRequest { ast, request });
        Ok(())
    }

    pub(crate) fn into_routed(&mut self, route: Route) -> Result<(), Error> {
        let request = match std::mem::replace(self, Self::Transitioning) {
            Self::Parsed(request) => request,
            request => {
                *self = request;
                return Err(Error::NoAst);
            }
        };

        *self = Self::Routed(RoutedEngineRequest { route, request });
        Ok(())
    }

    pub(crate) fn ast(&self) -> Result<&Option<Ast>, Error> {
        match self {
            Self::Initial(_) => Err(Error::NoAst),
            Self::Parsed(parsed) => Ok(&parsed.ast),
            Self::Routed(routed) => Ok(&routed.request.ast),
            Self::Transitioning => unreachable!(),
        }
    }

    pub(crate) fn route(&self) -> Result<&Route, Error> {
        match self {
            Self::Routed(routed) => Ok(&routed.route),
            _ => Err(Error::NoRoute),
        }
    }

    pub(crate) fn route_mut(&mut self) -> Result<&mut Route, Error> {
        match self {
            Self::Routed(routed) => Ok(&mut routed.route),
            _ => Err(Error::NoRoute),
        }
    }
}

impl Deref for EngineRequest<'_> {
    type Target = ClientRequest;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Initial(req) => req,
            Self::Parsed(parsed) => parsed.deref(),
            Self::Routed(routed) => routed.deref(),
            Self::Transitioning => unreachable!(),
        }
    }
}

impl DerefMut for EngineRequest<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Initial(req) => req,
            Self::Parsed(parsed) => parsed.deref_mut(),
            Self::Routed(routed) => routed.deref_mut(),
            Self::Transitioning => unreachable!(),
        }
    }
}

pub(crate) struct ParsedEngineRequest<'a> {
    pub(crate) ast: Option<Ast>,
    pub(crate) request: &'a mut ClientRequest,
}

pub(crate) struct RoutedEngineRequest<'a> {
    pub(crate) route: Route,
    pub(crate) request: ParsedEngineRequest<'a>,
}

impl Deref for ParsedEngineRequest<'_> {
    type Target = ClientRequest;

    fn deref(&self) -> &Self::Target {
        self.request
    }
}

impl DerefMut for ParsedEngineRequest<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.request
    }
}

impl Deref for RoutedEngineRequest<'_> {
    type Target = ClientRequest;

    fn deref(&self) -> &Self::Target {
        self.request.request
    }
}

impl DerefMut for RoutedEngineRequest<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.request.request
    }
}
