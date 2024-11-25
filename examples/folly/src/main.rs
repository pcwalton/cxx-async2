// cxx-async/examples/folly/src/main.rs
//
//! Demonstrates how to use Folly with `cxx_async`.

use crate::ffi::StringNamespaced;
use async_recursion::async_recursion;
use cxx_async::CxxAsyncException;
use futures::executor::{self, ThreadPool};
use futures::task::SpawnExt;
use futures::{join, Stream};
use futures::{StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;

#[cxx::bridge]
mod ffi {
    #[derive(Debug, PartialEq)]
    struct StringNamespaced {
        namespaced_string: String,
    }

    extern "Rust" {
        fn rust_hello() -> RustFutureVoid;
        fn rust_dot_product() -> RustFutureF64;
        fn rust_not_product() -> RustFutureF64;
        fn rust_folly_ping_pong(i: i32) -> RustFutureString;
    }

    unsafe extern "C++" {
        include!("folly_example.h");

        type RustFutureVoid = crate::RustFutureVoid;
        type RustFutureF64 = crate::RustFutureF64;
        type RustFutureString = crate::RustFutureString;
        #[namespace = foo::rust::bar]
        type RustFutureStringNamespaced = crate::RustFutureStringNamespaced;
        type RustStreamString = crate::RustStreamString;

        fn folly_dot_product_coro() -> RustFutureF64;
        fn folly_dot_product_futures() -> RustFutureF64;
        fn folly_get_namespaced_string() -> RustFutureStringNamespaced;
        fn folly_call_rust_hello();
        fn folly_call_rust_dot_product() -> f64;
        fn folly_schedule_rust_dot_product() -> f64;
        fn folly_not_product() -> RustFutureF64;
        fn folly_call_rust_not_product() -> String;
        fn folly_ping_pong(i: i32) -> RustFutureString;
        fn folly_complete() -> RustFutureVoid;
        fn folly_send_to_dropped_future_go();
        fn folly_send_to_dropped_future() -> RustFutureF64;
        fn folly_fizzbuzz() -> RustStreamString;
        fn folly_indirect_fizzbuzz() -> RustStreamString;
        fn folly_not_fizzbuzz() -> RustStreamString;
        fn folly_drop_coroutine_wait() -> RustFutureVoid;
        fn folly_drop_coroutine_signal() -> RustFutureVoid;
    }
}

#[cxx_async::bridge]
unsafe impl Future for RustFutureVoid {
    type Output = ();
}
#[cxx_async::bridge]
unsafe impl Future for RustFutureF64 {
    type Output = f64;
}
#[cxx_async::bridge]
unsafe impl Future for RustFutureString {
    type Output = String;
}
#[cxx_async::bridge(namespace = foo::rust::bar)]
unsafe impl Future for RustFutureStringNamespaced {
    type Output = StringNamespaced;
}
#[cxx_async::bridge]
unsafe impl Stream for RustStreamString {
    type Item = String;
}

const VECTOR_LENGTH: usize = 16384;
const SPLIT_LIMIT: usize = 32;

static THREAD_POOL: Lazy<ThreadPool> = Lazy::new(|| ThreadPool::new().unwrap());

static VECTORS: Lazy<(Vec<f64>, Vec<f64>)> = Lazy::new(|| {
    let mut rand = Xorshift::new();
    let (mut vector_a, mut vector_b) = (vec![], vec![]);
    for _ in 0..VECTOR_LENGTH {
        vector_a.push(rand.next() as f64);
        vector_b.push(rand.next() as f64);
    }
    (vector_a, vector_b)
});

struct Xorshift {
    state: u32,
}

impl Xorshift {
    fn new() -> Xorshift {
        Xorshift { state: 0x243f6a88 }
    }

    fn next(&mut self) -> u32 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.state = x;
        x
    }
}

fn rust_hello() -> RustFutureVoid {
    RustFutureVoid::infallible(async { println!("hello world") })
}

#[async_recursion]
async fn dot_product(range: Range<usize>) -> f64 {
    let len = range.end - range.start;
    if len > SPLIT_LIMIT {
        let mid = (range.start + range.end) / 2;
        let (first, second) = join!(
            THREAD_POOL
                .spawn_with_handle(dot_product(range.start..mid))
                .unwrap(),
            dot_product(mid..range.end)
        );
        return first + second;
    }

    let (ref a, ref b) = *VECTORS;
    range.clone().map(|index| a[index] * b[index]).sum()
}

fn rust_dot_product() -> RustFutureF64 {
    RustFutureF64::infallible(dot_product(0..VECTOR_LENGTH))
}

fn rust_not_product() -> RustFutureF64 {
    RustFutureF64::fallible(async {
        Err(CxxAsyncException::new("kapow".to_owned().into_boxed_str()))
    })
}

fn rust_folly_ping_pong(i: i32) -> RustFutureString {
    RustFutureString::infallible(async move {
        format!(
            "{}ping ",
            if i < 4 {
                ffi::folly_ping_pong(i + 1).await.unwrap()
            } else {
                "".to_owned()
            }
        )
    })
}

// Tests Rust calling C++ synchronously using the coroutine API.
#[test]
fn test_rust_calling_cpp_synchronously_coro() {
    assert_eq!(
        executor::block_on(ffi::folly_dot_product_coro()).unwrap(),
        75719554055754070000000.0
    );
}

// Tests Rust calling C++ synchronously using the Folly future combinator API.
#[test]
fn test_rust_calling_cpp_synchronously_futures() {
    assert_eq!(
        executor::block_on(ffi::folly_dot_product_futures()).unwrap(),
        75719554055754070000000.0
    );
    assert_eq!(
        executor::block_on(ffi::folly_get_namespaced_string())
            .unwrap()
            .namespaced_string,
        "hello world"
    );
}

// Tests Rust calling C++ on a scheduler.
#[test]
fn test_rust_calling_cpp_on_scheduler() {
    let future = ffi::folly_dot_product_coro();
    let value = executor::block_on(THREAD_POOL.spawn_with_handle(future).unwrap()).unwrap();
    assert_eq!(value, 75719554055754070000000.0);
    let future = ffi::folly_dot_product_futures();
    let value = executor::block_on(THREAD_POOL.spawn_with_handle(future).unwrap()).unwrap();
    assert_eq!(value, 75719554055754070000000.0);
}

// Tests C++ calling async Rust code that returns void synchronously.
#[test]
fn test_cpp_calling_void_rust_synchronously() {
    ffi::folly_call_rust_hello();
}

// Tests C++ calling async Rust code that returns non-void synchronously.
#[test]
fn test_cpp_calling_rust_synchronously() {
    assert_eq!(
        ffi::folly_call_rust_dot_product(),
        75719554055754070000000.0
    );
}

// Tests C++ calling async Rust code on a scheduler.
#[test]
fn test_cpp_calling_rust_on_scheduler() {
    assert_eq!(
        ffi::folly_schedule_rust_dot_product(),
        75719554055754070000000.0
    );
}

// Tests Rust calling async C++ code throwing exceptions.
#[test]
fn test_cpp_async_functions_throwing_exceptions() {
    match executor::block_on(ffi::folly_not_product()) {
        Ok(_) => panic!("shouldn't have succeeded"),
        Err(err) => assert_eq!(err.what(), "kaboom"),
    }
}

// Tests C++ calling async Rust code returning errors.
#[test]
fn test_rust_async_functions_returning_errors() {
    assert_eq!(ffi::folly_call_rust_not_product(), "kapow");
}

// Tests sending values across the language barrier synchronously.
#[test]
fn test_ping_pong() {
    let result = executor::block_on(ffi::folly_ping_pong(0)).unwrap();
    assert_eq!(result, "ping pong ping pong ping pong ping pong ping pong ");
}

// Test returning void.
#[test]
fn test_complete() {
    executor::block_on(ffi::folly_complete()).unwrap();
}

// Test dropping futures.
#[test]
fn test_dropping_futures() {
    ffi::folly_send_to_dropped_future();
    ffi::folly_send_to_dropped_future_go();
}

// Test Rust calling C++ streams.
#[test]
fn test_fizzbuzz() {
    let vector = executor::block_on(
        ffi::folly_fizzbuzz()
            .map(|result| result.unwrap())
            .collect::<Vec<String>>(),
    );
    assert_eq!(
        vector.join(", "),
        "1, 2, Fizz, 4, Buzz, Fizz, 7, 8, Fizz, Buzz, 11, Fizz, 13, 14, FizzBuzz"
    );
}

// Test Rust calling C++ streams that themselves internally await futures.
#[test]
fn test_indirect_fizzbuzz() {
    let vector = executor::block_on(
        ffi::folly_indirect_fizzbuzz()
            .map(|result| result.unwrap())
            .collect::<Vec<String>>(),
    );
    assert_eq!(
        vector.join(", "),
        "1, 2, Fizz, 4, Buzz, Fizz, 7, 8, Fizz, Buzz, 11, Fizz, 13, 14, FizzBuzz"
    );
}

#[test]
fn test_streams_throwing_exceptions() {
    let mut vector = executor::block_on(
        ffi::folly_not_fizzbuzz()
            .map_err(|err| err.what().to_owned())
            .collect::<Vec<Result<String, String>>>(),
    );
    assert_eq!(vector.pop().unwrap(), Err("kablam".to_owned()));
    let strings: Vec<String> = vector.into_iter().map(Result::unwrap).collect();
    assert_eq!(
        strings.join(", "),
        "1, 2, Fizz, 4, Buzz, Fizz, 7, 8, Fizz, Buzz"
    );
}

#[test]
fn test_dropping_coroutines() {
    // Make sure that coroutines get parented to the reaper so that destructors are called.
    let _ = ffi::folly_drop_coroutine_wait();
    drop(executor::block_on(ffi::folly_drop_coroutine_signal()));
}

fn main() {
    // Test Rust calling C++ async functions, both synchronously and via a scheduler.
    for fun in &[ffi::folly_dot_product_coro, ffi::folly_dot_product_futures] {
        let future = fun();
        println!("{}", executor::block_on(future).unwrap());
        let future = fun();
        println!(
            "{}",
            executor::block_on(THREAD_POOL.spawn_with_handle(future).unwrap()).unwrap()
        );
    }
    let future = ffi::folly_get_namespaced_string();
    println!("{:?}", executor::block_on(future).unwrap());

    // Test C++ calling Rust async functions.
    ffi::folly_call_rust_hello();
    ffi::folly_call_rust_dot_product();
    ffi::folly_schedule_rust_dot_product();

    // Test exceptions being thrown by C++ async functions.
    let future = ffi::folly_not_product();
    match executor::block_on(future) {
        Ok(_) => panic!("shouldn't have succeeded!"),
        Err(err) => println!("{}", err.what()),
    }

    // Test errors being thrown by Rust async functions.
    ffi::folly_call_rust_not_product();

    // Test yielding across the boundary repeatedly.
    let future = ffi::folly_ping_pong(0);
    println!("{}", executor::block_on(future).unwrap());

    // Test returning void.
    let future = ffi::folly_complete();
    executor::block_on(future).unwrap();

    // Test dropping futures.
    ffi::folly_send_to_dropped_future();
    ffi::folly_send_to_dropped_future_go();

    // Test Rust calling C++ streams.
    let vector = executor::block_on(
        ffi::folly_fizzbuzz()
            .map(|result| result.unwrap())
            .collect::<Vec<String>>(),
    );
    println!("{}", vector.join(", "));
    let vector = executor::block_on(
        ffi::folly_indirect_fizzbuzz()
            .map(|result| result.unwrap())
            .collect::<Vec<String>>(),
    );
    println!("{}", vector.join(", "));

    // Test Rust calling C++ streams that throw exceptions partway through.
    let vector = executor::block_on(
        ffi::folly_not_fizzbuzz()
            .map(|result| format!("{:?}", result))
            .collect::<Vec<String>>(),
    );
    println!("{}", vector.join(", "));

    // Test that destructors are called when dropping a future.
    let _ = ffi::folly_drop_coroutine_wait();
    drop(executor::block_on(ffi::folly_drop_coroutine_signal()));
}
