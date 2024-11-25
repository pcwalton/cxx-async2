/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under both the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree and the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree.
 */

// cxx-async/src/lib.rs
//
//! `cxx-async` is a Rust crate that extends the [`cxx`](http://cxx.rs/) library to provide
//! seamless interoperability between asynchronous Rust code using `async`/`await` and [C++20
//! coroutines] using `co_await`. If your C++ code is asynchronous, `cxx-async` can provide a more
//! convenient, and potentially more efficient, alternative to callbacks. You can freely convert
//! between C++ coroutines and Rust futures and await one from the other.
//!
//! It's important to emphasize what `cxx-async` isn't: it isn't a C++ binding to Tokio or any
//! other Rust I/O library. Nor is it a Rust binding to `boost::asio` or similar. Such bindings
//! could in principle be layered on top of `cxx-async` if desired, but this crate doesn't provide
//! them out of the box. (Note that this is a tricky problem even in theory, since Rust async I/O
//! code is generally tightly coupled to a single library such as Tokio, in much the same way C++
//! async I/O code tends to be tightly coupled to libraries like `boost::asio`.) If you're writing
//! server code, you can still use `cxx-async`, but you will need to ensure that both the Rust and
//! C++ sides run separate I/O executors.
//!
//! `cxx-async` aims for compatibility with popular C++ coroutine support libraries. Right now,
//! both the lightweight [`cppcoro`](https://github.com/lewissbaker/cppcoro) and the more
//! comprehensive [Folly](https://github.com/facebook/folly/) are supported. Pull requests are
//! welcome to support others.
//!
//! ## Quick tutorial
//!
//! To use `cxx-async`, first start by adding `cxx` to your project. Then add the following to your
//! `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! cxx-async = "0.1"
//! ```
//!
//! Now, inside your `#[cxx::bridge]` module, declare a future type and some methods like so:
//!
//! ```ignore
//! #[cxx::bridge]
//! mod ffi {
//!     // Declare type aliases for each of the future types you wish to use here. Then declare
//!     // async C++ methods that you wish Rust to call. Make sure they return one of the future
//!     // types you declared.
//!     unsafe extern "C++" {
//!         type RustFutureString = crate::RustFutureString;
//!
//!         fn hello_from_cpp() -> RustFutureString;
//!     }
//!
//!     // Async Rust methods that you wish C++ to call go here. Again, make sure they return one of
//!     // the future types you declared above.
//!     extern "Rust" {
//!         fn hello_from_rust() -> RustFutureString;
//!     }
//! }
//! ```
//!
//! After the `#[cxx::bridge]` block, define the future types using the
//! `#[cxx_async::bridge]` attribute:
//!
//! ```ignore
//! // The `Output` type is the Rust type that this future yields.
//! #[cxx_async::bridge]
//! unsafe impl Future for RustFutureString {
//!     type Output = String;
//! }
//! ```
//!
//! Note that it's your responsibility to ensure that the type you specify for Output actually
//! matches the type of the value that your future resolves to. Otherwise, undefined behavior can
//! result.
//!
//! Next, in your C++ header, make sure to `#include` the right headers:
//!
//! ```cpp
//! #include "rust/cxx.h"
//! #include "rust/cxx_async.h"
//! #include "rust/cxx_async_cppcoro.h"  // Or cxx_async_folly.h, as appropriate.
//! ```
//!
//! And add a call to the `CXXASYNC_DEFINE_FUTURE` macro in your headers to define the C++ side of
//! the future:
//!
//! ```cpp
//! // The first argument is the C++ type that the future yields, and the second argument is the
//! // fully-qualified name of the future, with `::` namespace separators replaced with commas. (For
//! // instance, if your future is named `mycompany::myproject::RustFutureString`, you might write
//! // `CXXASYNC_DEFINE_FUTURE(rust::String, mycompany, myproject, RustFutureString);`. The first
//! // argument is the C++ type that `cxx` maps your Rust type to: in this case, `String` maps to
//! // `rust::String`, so we supply `rust::String` here.
//! //
//! // Note that, because the C preprocessor doesn't know about the `<` and `>` brackets that
//! // surround template arguments, a template type that contains multiple arguments (e.g.
//! // `std::pair<int, std::string>`) will need to be factored out into a `typedef` to be used
//! // inside `CXXASYNC_DEFINE_FUTURE`. Otherwise, the C preprocessor won't parse it properly.
//! //
//! // This macro must be invoked at the top level, not in a namespace.
//! CXXASYNC_DEFINE_FUTURE(rust::String, RustFutureString);
//! ```
//!
//! You're done! Now you can define asynchronous C++ code that Rust can call:
//!
//! ```cpp
//! RustFutureString hello_from_cpp() {
//!     co_return std::string("Hello world!");
//! }
//! ```
//!
//! On the Rust side:
//!
//! ```ignore
//! async fn call_cpp() -> String {
//!     // This returns a Result (with the error variant populated if C++ threw an exception), so
//!     // you need to unwrap it:
//!     ffi::hello_from_cpp().await.unwrap()
//! }
//! ```
//!
//! And likewise, define some asynchronous Rust code that C++ can call:
//!
//! ```ignore
//! use cxx_async::CxxAsyncResult;
//! fn hello_from_rust() -> RustFutureString {
//!     // You can instead use `fallible` if your async block returns a Result.
//!     RustFutureString::infallible(async { "Hello world!".to_owned() })
//! }
//! ```
//!
//! Over on the C++ side:
//!
//! ```cpp
//! cppcoro::task<rust::String> call_rust() {
//!     co_return hello_from_rust();
//! }
//! ```
//!
//! In this way, you should now be able to freely await futures on either side.
//!
//! [C++20 coroutines]: https://en.cppreference.com/w/cpp/language/coroutines

#![warn(missing_docs)]

extern crate link_cplusplus;

use crate::execlet::Execlet;
use crate::execlet::ExecletReaper;
use crate::execlet::RustExeclet;
use futures::Stream;
use futures::StreamExt;
use std::convert::From;
use std::error::Error;
use std::ffi::CStr;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::future::Future;
use std::io;
use std::io::Write;
use std::os::raw::c_char;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::process;
use std::ptr;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;

const FUTURE_STATUS_PENDING: u32 = 0;
const FUTURE_STATUS_COMPLETE: u32 = 1;
const FUTURE_STATUS_ERROR: u32 = 2;
const FUTURE_STATUS_RUNNING: u32 = 3;

const SEND_RESULT_WAIT: u32 = 0;
const SEND_RESULT_SENT: u32 = 1;
const SEND_RESULT_FINISHED: u32 = 2;

pub use cxx_async_macro::bridge;

// Replacements for macros that panic that are guaranteed to cause an abort, so that we don't unwind
// across C++ frames.

macro_rules! safe_panic {
    ($($args:expr),*) => {
        {
            use ::std::io::Write;
            drop(write!(::std::io::stderr(), $($args),*));
            drop(writeln!(::std::io::stderr(), " at {}:{}", file!(), line!()));
            ::std::process::abort();
        }
    }
}

#[cfg(debug_assertions)]
macro_rules! safe_debug_assert {
    ($cond:expr) => {{
        use ::std::io::Write;
        if !$cond {
            drop(writeln!(
                ::std::io::stderr(),
                "assertion failed: {}",
                stringify!($cond)
            ));
            ::std::process::abort();
        }
    }};
}
#[cfg(not(debug_assertions))]
macro_rules! safe_debug_assert {
    ($cond:expr) => {};
}

macro_rules! safe_unreachable {
    () => {
        safe_panic!("unreachable code executed")
    };
}

trait SafeExpect {
    type Output;
    fn safe_expect(self, message: &str) -> Self::Output;
}

impl<T> SafeExpect for Option<T> {
    type Output = T;
    fn safe_expect(self, message: &str) -> T {
        match self {
            Some(value) => value,
            None => safe_panic!("{}", message),
        }
    }
}

trait SafeUnwrap {
    type Output;
    fn safe_unwrap(self) -> Self::Output;
}

impl<T, E> SafeUnwrap for Result<T, E>
where
    E: Debug,
{
    type Output = T;
    fn safe_unwrap(self) -> Self::Output {
        match self {
            Ok(value) => value,
            Err(error) => safe_panic!("unexpected Result::Err: {:?}", error),
        }
    }
}

#[doc(hidden)]
pub mod execlet;

// Bridged glue functions.
extern "C" {
    fn cxxasync_suspended_coroutine_clone(waker_data: *mut u8) -> *mut u8;
    fn cxxasync_suspended_coroutine_wake(waker_data: *mut u8);
    fn cxxasync_suspended_coroutine_wake_by_ref(waker_data: *mut u8);
    fn cxxasync_suspended_coroutine_drop(waker_data: *mut u8);
}

// A suspended C++ coroutine needs to act as a waker if it awaits a Rust future. This vtable
// provides that glue.
static CXXASYNC_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    rust_suspended_coroutine_clone,
    rust_suspended_coroutine_wake,
    rust_suspended_coroutine_wake_by_ref,
    rust_suspended_coroutine_drop,
);

/// Any exception that a C++ coroutine throws is automatically caught and converted into this error
/// type.
///
/// This is just a wrapper around the result of `std::exception::what()`.
#[derive(Debug)]
pub struct CxxAsyncException {
    what: Box<str>,
}

impl CxxAsyncException {
    /// Creates a new exception with the given error message.
    pub fn new(what: Box<str>) -> Self {
        Self { what }
    }

    /// The value returned by `std::exception::what()`.
    pub fn what(&self) -> &str {
        &self.what
    }
}

impl Display for CxxAsyncException {
    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
        formatter.write_str(&self.what)
    }
}

impl Error for CxxAsyncException {}

/// A convenient shorthand for `Result<T, CxxAsyncException>`.
pub type CxxAsyncResult<T> = Result<T, CxxAsyncException>;

// A table of functions that the `bridge` macro emits for the C++ bridge to use.
//
// This must match the definition in `cxx_async.h`.
#[repr(C)]
#[doc(hidden)]
pub struct CxxAsyncVtable {
    pub channel: *mut u8,
    pub sender_send: *mut u8,
    pub sender_drop: *mut u8,
    pub future_poll: *mut u8,
    pub future_drop: *mut u8,
}

unsafe impl Send for CxxAsyncVtable {}
unsafe impl Sync for CxxAsyncVtable {}

// A sender/receiver pair for the return value of a wrapped one-shot C++ coroutine.
//
// This is an implementation detail and is not exposed to the programmer. It must match the
// definition in `cxx_async.h`.
#[repr(C)]
#[doc(hidden)]
pub struct CxxAsyncFutureChannel<Fut, Out> {
    // The receiving end.
    future: Fut,
    // The sending end.
    sender: CxxAsyncSender<Out>,
}

// A sender/receiver pair for the return value of a wrapped C++ multi-shot coroutine.
//
// This is an implementation detail and is not exposed to the programmer. It must match the
// definition in `cxx_async.h`.
#[repr(C)]
#[doc(hidden)]
pub struct CxxAsyncStreamChannel<Stm, Item>
where
    Stm: Stream<Item = CxxAsyncResult<Item>>,
{
    // The receiving end.
    future: Stm,
    // The sending end.
    sender: CxxAsyncSender<Item>,
}

// The single-producer/single-consumer channel type that future and stream implementations use to
// pass values between the two languages.
//
// We can't use a `futures::channel::mpsc` channel because it can deadlock. With a standard MPSC
// channel, if we try to send a value when the buffer is full and the receiving end is woken up and
// then tries to receive the value, a deadlock occurs, as the MPSC channel doesn't drop locks before
// calling the waker.
struct SpscChannel<T>(Arc<Mutex<SpscChannelImpl<T>>>);

// Data for each SPSC channel.
struct SpscChannelImpl<T> {
    // The waker waiting on the value.
    //
    // This can either be the sending end waiting for the receiving end to receive a previously-sent
    // value (for streams only) or the receiving end waiting for the sending end to post a value.
    waiter: Option<Waker>,
    // The value waiting to be read.
    value: Option<T>,
    // An exception from the C++ side that is to be delivered over to the Rust side.
    exception: Option<CxxAsyncException>,
    // True if the channel is closed; false otherwise.
    closed: bool,
}

impl<T> SpscChannel<T> {
    // Creates a new SPSC channel.
    fn new() -> SpscChannel<T> {
        SpscChannel(Arc::new(Mutex::new(SpscChannelImpl {
            waiter: None,
            value: None,
            exception: None,
            closed: false,
        })))
    }

    // Marks the channel as closed. Only the sending end may call this.
    fn close(&self) {
        // Drop the lock before possibly calling the waiter because we could deadlock otherwise.
        let waiter;
        {
            let mut this = self.0.lock().safe_unwrap();
            if this.closed {
                safe_panic!("Attempted to close an `SpscChannel` that's already closed!")
            }
            this.closed = true;
            waiter = this.waiter.take();
        }

        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    // Attempts to send a value. If this channel already has a value yet to be read, this function
    // returns false. Otherwise, it calls the provided closure to retrieve the value and returns
    // true.
    //
    // This callback-based design eliminates the requirement to return the original value if the
    // send fails.
    fn try_send_value_with<F>(&self, context: Option<&Context>, getter: F) -> bool
    where
        F: FnOnce() -> T,
    {
        // Drop the lock before possibly calling the waiter because we could deadlock otherwise.
        let waiter;
        {
            let mut this = self.0.lock().safe_unwrap();
            if this.value.is_none() {
                this.value = Some(getter());
                waiter = this.waiter.take();
            } else if context.is_some() && this.waiter.is_some() {
                safe_panic!("Only one task may block on a `SpscChannel`!")
            } else {
                if let Some(context) = context {
                    this.waiter = Some((*context.waker()).clone());
                }
                return false;
            }
        }

        if let Some(waiter) = waiter {
            waiter.wake();
        }
        true
    }

    // Raises an exception. This is synchronous and thus should never fail. It must only be called
    // once, or not at all, for a given `SpscSender`.
    fn send_exception(&self, exception: CxxAsyncException) {
        // Drop the lock before possibly calling the waiter because we could deadlock otherwise.
        let waiter = {
            let mut this = self.0.lock().safe_unwrap();
            safe_debug_assert!(this.exception.is_none());
            this.exception = Some(exception);
            this.waiter.take()
        };

        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    // Attempts to receive a value. Returns `Poll::Pending` if no value is available and the channel
    // isn't closed. Returns `Poll::Ready(None)` if the channel is closed. Otherwise, receives a
    // value and returns `Poll::Ready(Some)`.
    fn recv(&self, cx: &Context) -> Poll<Option<CxxAsyncResult<T>>> {
        // Drop the lock before possibly calling the waiter because we could deadlock otherwise.
        let (result, waiter);
        {
            let mut this = self.0.lock().safe_unwrap();
            match this.value.take() {
                Some(value) => {
                    result = Ok(value);
                    waiter = this.waiter.take();
                }
                None => match this.exception.take() {
                    Some(exception) => {
                        result = Err(exception);
                        waiter = this.waiter.take();
                    }
                    None if this.closed => return Poll::Ready(None),
                    None => {
                        this.waiter = Some((*cx.waker()).clone());
                        return Poll::Pending;
                    }
                },
            }
        }

        if let Some(waiter) = waiter {
            waiter.wake();
        }
        Poll::Ready(Some(result))
    }
}

impl<T> Clone for SpscChannel<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

// The concrete type of the stream that wraps a C++ coroutine, either one-shot (future) or
// multi-shot (stream).
//
// The programmer only interacts with this abstractly behind a `Box<dyn Future>` or
// `Box<dyn Stream>` trait object, so this type is considered an implementation detail. It must be
// public because the `bridge_stream` macro needs to name it.
#[doc(hidden)]
pub struct CxxAsyncReceiver<Item> {
    // The SPSC channel to receive on.
    receiver: SpscChannel<Item>,
    // Any execlet that must be driven when receiving.
    execlet: Option<Execlet>,
}

// The concrete type of the sending end of a stream.
//
// This must be public because the `bridge_stream` macro needs to name it.
#[doc(hidden)]
#[repr(transparent)]
pub struct CxxAsyncSender<Item>(*mut SpscChannel<Item>);

impl<Item> Drop for CxxAsyncSender<Item> {
    fn drop(&mut self) {
        unsafe { drop(Box::from_raw(self.0)) }
    }
}

// This is a little weird in that `CxxAsyncReceiver` behaves as a oneshot if it's treated as a
// Future and an SPSC stream if it's treated as a Stream. But since the programmer only ever
// interacts with these objects behind boxed trait objects that only expose one of the two traits,
// it's not a problem.
impl<Item> Stream for CxxAsyncReceiver<Item> {
    type Item = CxxAsyncResult<Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(ref execlet) = self.execlet {
            execlet.run(cx);
        }
        self.receiver.recv(cx)
    }
}

impl<Output> Future for CxxAsyncReceiver<Output> {
    type Output = CxxAsyncResult<Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(ref execlet) = self.execlet {
            execlet.run(cx);
        }
        match self.receiver.recv(cx) {
            Poll::Ready(Some(Ok(value))) => Poll::Ready(Ok(value)),
            Poll::Ready(Some(Err(exception))) => Poll::Ready(Err(exception)),
            Poll::Ready(None) => {
                // This should never happen, because a future should never be polled again after
                // returning `Ready`.
                safe_panic!("Attempted to use a stream as a future!")
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<Item> From<SpscChannel<Item>> for CxxAsyncReceiver<Item> {
    fn from(receiver: SpscChannel<Item>) -> Self {
        CxxAsyncReceiver {
            receiver,
            execlet: None,
        }
    }
}

impl<Item> Drop for CxxAsyncReceiver<Item> {
    fn drop(&mut self) {
        let execlet = match self.execlet {
            Some(ref execlet) => execlet,
            None => return,
        };
        let receiver = self.receiver.0.lock().safe_unwrap();
        if receiver.closed {
            return;
        }
        ExecletReaper::get().add((*execlet).clone());
    }
}

// The sending end that the C++ bridge uses to return a value to a Rust future.
//
// This is an implementation detail.
#[doc(hidden)]
pub trait RustSender {
    type Output;
    fn send(&mut self, value: CxxAsyncResult<Self::Output>);
}

/// Wraps an arbitrary Rust Future in a boxed `cxx-async` future so that it can be returned to C++.
///
/// You should not need to implement this manually; it's automatically implemented by the `bridge`
/// macro.
pub trait IntoCxxAsyncFuture: Sized {
    /// The type of the value yielded by the future.
    type Output;

    /// Wraps a Rust Future that directly returns the output type.
    ///
    /// Use this when you aren't interested in propagating errors to C++ as exceptions.
    fn infallible<Fut>(future: Fut) -> Self
    where
        Fut: Future<Output = Self::Output> + Send + 'static,
    {
        Self::fallible(async move { Ok(future.await) })
    }

    /// Wraps a Rust Future that returns the output type, wrapped in a `CxxAsyncResult`.
    ///
    /// Use this when you have error values that you want to turn into exceptions on the C++ side.
    fn fallible<Fut>(future: Fut) -> Self
    where
        Fut: Future<Output = CxxAsyncResult<Self::Output>> + Send + 'static;
}

/// Wraps an arbitrary Rust Stream in a boxed `cxx-async` stream so that it can be returned to C++.
///
/// You should not need to implement this manually; it's automatically implemented by the
/// `bridge_stream` macro.
pub trait IntoCxxAsyncStream: Sized {
    /// The type of the values yielded by the stream.
    type Item;

    /// Wraps a Rust Stream that directly yields items of the output type.
    ///
    /// Use this when you aren't interested in propagating errors to C++ as exceptions.
    fn infallible<Stm>(stream: Stm) -> Self
    where
        Stm: Stream<Item = Self::Item> + Send + 'static,
        Stm::Item: 'static,
    {
        Self::fallible(stream.map(Ok))
    }

    /// Wraps a Rust Stream that yields items of the output type, wrapped in `CxxAsyncResult`s.
    ///
    /// Use this when you have error values that you want to turn into exceptions on the C++ side.
    fn fallible<Stm>(stream: Stm) -> Self
    where
        Stm: Stream<Item = CxxAsyncResult<Self::Item>> + Send + 'static;
}

// Creates a new oneshot sender/receiver pair for a future.
//
// SAFETY: This is a raw FFI function called by our C++ code.
//
// This needs an out pointer because of https://github.com/rust-lang/rust-bindgen/issues/778
#[doc(hidden)]
pub unsafe extern "C" fn future_channel<Fut, Out>(
    out_oneshot: *mut CxxAsyncFutureChannel<Fut, Out>,
    execlet: *mut RustExeclet,
) where
    Fut: From<CxxAsyncReceiver<Out>> + Future<Output = CxxAsyncResult<Out>>,
{
    let channel = SpscChannel::new();
    let oneshot = CxxAsyncFutureChannel {
        sender: CxxAsyncSender(Box::into_raw(Box::new(channel.clone()))),
        future: CxxAsyncReceiver::<Out> {
            receiver: channel,
            execlet: Some(Execlet::from_raw_ref(execlet)),
        }
        .into(),
    };
    ptr::write(out_oneshot, oneshot);
}

// Creates a new multi-shot sender/receiver pair for a stream.
//
// SAFETY: This is a raw FFI function called by our C++ code.
//
// This needs an out pointer because of https://github.com/rust-lang/rust-bindgen/issues/778
#[doc(hidden)]
pub unsafe extern "C" fn stream_channel<Stm, Item>(
    out_stream: *mut CxxAsyncStreamChannel<Stm, Item>,
    execlet: *mut RustExeclet,
) where
    Stm: From<CxxAsyncReceiver<Item>> + Stream<Item = CxxAsyncResult<Item>>,
{
    let channel = SpscChannel::new();
    let stream = CxxAsyncStreamChannel {
        sender: CxxAsyncSender(Box::into_raw(Box::new(channel.clone()))),
        future: CxxAsyncReceiver {
            receiver: channel,
            execlet: Some(Execlet::from_raw_ref(execlet)),
        }
        .into(),
    };
    ptr::write(out_stream, stream);
}

// C++ calls this to yield a value for a one-shot coroutine (future).
//
// SAFETY: This is a low-level function called by our C++ code.
//
// Takes ownership of the value. The caller must not call its destructor.
//
// This function always closes the channel and returns `SEND_RESULT_FINISHED`.
//
// If `status` is `FUTURE_STATUS_COMPLETE`, then the given value is sent; otherwise, if `status` is
// `FUTURE_STATUS_ERROR`, `value` must point to an exception string. `FUTURE_STATUS_RUNNING` is
// illegal, because that value is only for streams, not futures.
//
// The `waker_data` parameter should always be null, because a one-shot coroutine should never
// block on yielding a value.
//
// Any errors when sending are dropped on the floor. This is the right behavior because futures
// can be legally dropped in Rust to signal cancellation.
#[doc(hidden)]
pub unsafe extern "C" fn sender_future_send<Item>(
    this: &mut CxxAsyncSender<Item>,
    status: u32,
    value: *const u8,
    waker_data: *const u8,
) -> u32 {
    safe_debug_assert!(waker_data.is_null());

    let this = this.0.as_mut().safe_expect("Where's the SPSC sender?");
    match status {
        FUTURE_STATUS_COMPLETE => {
            // This is a one-shot sender, so sending must always succeed.
            let sent = this.try_send_value_with(None, || ptr::read(value as *const Item));
            safe_debug_assert!(sent);
        }
        FUTURE_STATUS_ERROR => this.send_exception(unpack_exception(value)),
        _ => safe_unreachable!(),
    }

    this.close();
    SEND_RESULT_FINISHED
}

// C++ calls this to yield a value for a multi-shot coroutine (stream).
//
// SAFETY: This is a low-level function called by our C++ code.
//
// Takes ownership of the value if and only if it was successfully sent (otherwise, leaves it
// alone). The caller must not call its destructor on a successful send.
//
// This function returns `SEND_RESULT_SENT` if the value was successfully sent, `SEND_RESULT_WAIT`
// if the value wasn't sent because another value was already in the slot (in which case the task
// will need to go to sleep), and `SEND_RESULT_FINISHED` if the channel was closed.
//
// If `status` is `FUTURE_STATUS_COMPLETE`, then the channel is closed, the value is ignored, and
// this function immediately returns `SEND_RESULT_FINISHED`. Note that this behavior is different
// from `sender_future_send`, which actually sends the value if `status` is
// `FUTURE_STATUS_COMPLETE`.
//
// If `waker_data` is present, this identifies the coroutine handle that will be awakened if the
// channel is currently full.
//
// Any errors when sending are dropped on the floor. This is because futures can be legally dropped
// in Rust to signal cancellation.
#[doc(hidden)]
pub unsafe extern "C" fn sender_stream_send<Item>(
    this: &mut CxxAsyncSender<Item>,
    status: u32,
    value: *const u8,
    waker_data: *const u8,
) -> u32 {
    let (waker, context);
    if waker_data.is_null() {
        context = None;
    } else {
        waker = Waker::from_raw(RawWaker::new(
            waker_data as *const (),
            &CXXASYNC_WAKER_VTABLE,
        ));
        context = Some(Context::from_waker(&waker));
    }

    let this = this.0.as_mut().safe_expect("Where's the SPSC sender?");
    match status {
        FUTURE_STATUS_COMPLETE => {
            this.close();
            SEND_RESULT_FINISHED
        }
        FUTURE_STATUS_RUNNING => {
            let sent =
                this.try_send_value_with(context.as_ref(), || ptr::read(value as *const Item));
            if sent {
                SEND_RESULT_SENT
            } else {
                SEND_RESULT_WAIT
            }
        }
        FUTURE_STATUS_ERROR => {
            this.send_exception(unpack_exception(value));
            this.close();
            SEND_RESULT_FINISHED
        }
        _ => safe_unreachable!(),
    }
}

// C++ calls this to destroy a sender.
//
// SAFETY: This is a low-level function called by our C++ code.
#[doc(hidden)]
pub unsafe extern "C" fn sender_drop<Item>(_: CxxAsyncSender<Item>) {
    // Destructor automatically runs.
}

unsafe fn unpack_exception(value: *const u8) -> CxxAsyncException {
    let string = CStr::from_ptr(value as *const c_char);
    CxxAsyncException::new(string.to_string_lossy().into_owned().into_boxed_str())
}

// C++ calls this to poll a wrapped Rust future.
//
// SAFETY:
// * This is a low-level function called by our C++ code.
// * `Pin<&mut Future>` is marked `#[repr(transparent)]`, so it's FFI-safe.
// * We catch all panics inside `poll` so that they don't unwind into C++.
#[doc(hidden)]
pub unsafe extern "C" fn future_poll<Fut, Out>(
    this: Pin<&mut Fut>,
    result: *mut u8,
    waker_data: *const u8,
) -> u32
where
    Fut: Future<Output = CxxAsyncResult<Out>>,
{
    let waker = Waker::from_raw(RawWaker::new(
        waker_data as *const (),
        &CXXASYNC_WAKER_VTABLE,
    ));

    let result = panic::catch_unwind(AssertUnwindSafe(move || {
        let mut context = Context::from_waker(&waker);
        match this.poll(&mut context) {
            Poll::Ready(Ok(value)) => {
                ptr::write(result as *mut Out, value);
                FUTURE_STATUS_COMPLETE
            }
            Poll::Ready(Err(error)) => {
                let error = error.what().to_owned();
                ptr::write(result as *mut String, error);
                FUTURE_STATUS_ERROR
            }
            Poll::Pending => FUTURE_STATUS_PENDING,
        }
    }));

    match result {
        Ok(result) => result,
        Err(error) => {
            drop(writeln!(
                io::stderr(),
                "Rust async code panicked when awaited from C++: {:?}",
                error
            ));
            process::abort();
        }
    }
}

// C++ calls this to drop a Rust future.
//
// SAFETY:
// * This is a low-level function called by our C++ code.
#[doc(hidden)]
pub unsafe extern "C" fn future_drop<Fut>(future: *mut Fut) {
    ptr::drop_in_place(future);
}

// Bumps the reference count on a suspended C++ coroutine.
//
// SAFETY: This is a raw FFI function called by the currently-running Rust executor.
unsafe fn rust_suspended_coroutine_clone(address: *const ()) -> RawWaker {
    RawWaker::new(
        cxxasync_suspended_coroutine_clone(address as *mut () as *mut u8) as *mut () as *const (),
        &CXXASYNC_WAKER_VTABLE,
    )
}

// Resumes a suspended C++ coroutine and decrements its reference count.
//
// SAFETY: This is a raw FFI function called by the currently-running Rust executor.
unsafe fn rust_suspended_coroutine_wake(address: *const ()) {
    cxxasync_suspended_coroutine_wake(address as *mut () as *mut u8)
}

// Resumes a suspended C++ coroutine without decrementing its reference count.
//
// SAFETY: This is a raw FFI function called by the currently-running Rust executor.
unsafe fn rust_suspended_coroutine_wake_by_ref(address: *const ()) {
    cxxasync_suspended_coroutine_wake_by_ref(address as *mut () as *mut u8)
}

// Decrements the reference count on a suspended C++ coroutine.
//
// SAFETY: This is a raw FFI function called by the currently-running Rust executor.
unsafe fn rust_suspended_coroutine_drop(address: *const ()) {
    cxxasync_suspended_coroutine_drop(address as *mut () as *mut u8)
}

// Reexports for the `#[bridge]` macro to use internally. Users of this crate shouldn't use these;
// they should import the `futures` crate directly.
#[doc(hidden)]
pub mod private {
    pub use futures::future::BoxFuture;
    pub use futures::stream::BoxStream;
}
