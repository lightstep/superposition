//! A clone of the waker_fn crate, with the modification that the waker function is neither Send
//! nor Sync. I also replaced Arc with Rc.
//!
//! Original comments:
//!
//! Convert closures into wakers.
//!
//! A [`Waker`] is just a fancy callback. This crate converts regular closures into wakers.

use std::mem::{self, ManuallyDrop};
use std::rc::Rc;
use std::task::{RawWaker, RawWakerVTable, Waker};

/// Original comments:
///
/// Converts a closure into a [`Waker`].
///
/// The closure gets called every time the waker is woken.
#[inline]
pub fn waker_fn<F: Fn()>(f: F) -> Waker {
    let raw = Rc::into_raw(Rc::new(f)) as *const ();
    let vtable = &Helper::<F>::VTABLE;
    unsafe { Waker::from_raw(RawWaker::new(raw, vtable)) }
}

struct Helper<F>(F);

impl<F: Fn()> Helper<F> {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_waker,
        Self::wake,
        Self::wake_by_ref,
        Self::drop_waker,
    );

    unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
        let rc = ManuallyDrop::new(Rc::from_raw(ptr as *const F));
        mem::forget(rc.clone());
        RawWaker::new(ptr, &Self::VTABLE)
    }

    unsafe fn wake(ptr: *const ()) {
        let rc = Rc::from_raw(ptr as *const F);
        (rc)();
    }

    unsafe fn wake_by_ref(ptr: *const ()) {
        let rc = ManuallyDrop::new(Rc::from_raw(ptr as *const F));
        (rc)();
    }

    unsafe fn drop_waker(ptr: *const ()) {
        drop(Rc::from_raw(ptr as *const F));
    }
}
