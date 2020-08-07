use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

pin_project! {
    /// Synchronously execute a callback function when the stored future polls Ready.
    pub struct OnReadyFn<F, G> {
        #[pin]
        fut: F,
        callback: Option<G>,
    }
}

impl<F, G> OnReadyFn<F, G> {
    pub fn new(fut: F, callback: G) -> Self {
        let callback = Some(callback);
        Self { fut, callback }
    }
}

impl<F, G> Future for OnReadyFn<F, G>
where
    F: Future,
    G: FnOnce(),
{
    type Output = <F as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.fut.poll(cx) {
            Poll::Pending => Poll::Pending,
            ret @ Poll::Ready(_) => {
                if let Some(g) = this.callback.take() {
                    g();
                }
                ret
            }
        }
    }
}
