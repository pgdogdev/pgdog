//! A trait object used to index into a map-ish container using a tuple key.
//! This trait is needed when we want to use borrowed types to look up owned
//! data. There is no easy way to have a type which is semantically (String,
//! String) implement Borrow<(&str, &str)>. The signature of Borrow would
//! require us to manifest a tuple reference out of thin air.
//!
//! Instead we use a trait object which lets us erase the type entirely,
//! creating a reference to each element of the tuple when needed for comparison
//! and hashing.
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub(crate) trait KeyPair<A: ?Sized, B: ?Sized> {
    fn left(&self) -> &A;
    fn right(&self) -> &B;
}

impl<A, B> KeyPair<A, B> for (A, B) {
    fn left(&self) -> &A {
        &self.0
    }

    fn right(&self) -> &B {
        &self.1
    }
}

impl<A: ?Sized, B: ?Sized> KeyPair<A, B> for (&A, &B) {
    fn left(&self) -> &A {
        self.0
    }

    fn right(&self) -> &B {
        self.1
    }
}

impl<A: ?Sized, B> KeyPair<A, B> for (&A, B) {
    fn left(&self) -> &A {
        self.0
    }

    fn right(&self) -> &B {
        &self.1
    }
}

impl<A, B: ?Sized> KeyPair<A, B> for (A, &B) {
    fn left(&self) -> &A {
        &self.0
    }

    fn right(&self) -> &B {
        self.1
    }
}

impl<A: ?Sized, B: ?Sized> KeyPair<A, B> for (Arc<A>, B) {
    fn left(&self) -> &A {
        &self.0
    }

    fn right(&self) -> &B {
        &self.1
    }
}

impl<'a, A, B, X, Y> Borrow<dyn KeyPair<A, B> + 'a> for (X, Y)
where
    A: ?Sized,
    B: ?Sized,
    (X, Y): KeyPair<A, B> + 'a,
{
    fn borrow(&self) -> &(dyn KeyPair<A, B> + 'a) {
        self
    }
}

impl<'a, A, B> PartialEq for dyn KeyPair<A, B> + 'a
where
    A: PartialEq + ?Sized + 'a,
    B: PartialEq + ?Sized + 'a,
{
    fn eq(&self, other: &Self) -> bool {
        self.left() == other.left() && self.right() == other.right()
    }
}

impl<'a, A, B> Eq for dyn KeyPair<A, B> + 'a
where
    A: Eq + ?Sized + 'a,
    B: Eq + ?Sized + 'a,
{
}

impl<'a, A, B> Hash for dyn KeyPair<A, B> + 'a
where
    A: ?Sized + 'a,
    B: ?Sized + 'a,
    for<'b, 'c> (&'b A, &'c B): Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.left(), self.right()).hash(state)
    }
}
