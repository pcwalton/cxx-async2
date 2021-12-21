/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under both the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree and the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree.
 */

// cxx-async/include/rust/cxx_async.h

#ifndef RUST_CXX_ASYNC_H
#define RUST_CXX_ASYNC_H

#include <atomic>
#include <cstdint>
#include <cstring>
#include <exception>
#include <experimental/coroutine>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include "rust/cxx.h"

#define CXXASYNC_ASSERT(cond) ::rust::async::cxxasync_assert(cond)

#define CXXASYNC_DEFINE_FUTURE(name, type)     \
  template <>                                  \
  struct rust::async::RustFutureTraits<name> { \
    typedef type YieldResult;                  \
    typedef type FinalResult;                  \
  };
#define CXXASYNC_DEFINE_STREAM(name, type)     \
  template <>                                  \
  struct rust::async::RustFutureTraits<name> { \
    typedef type YieldResult;                  \
    typedef void FinalResult;                  \
  };

namespace rust {
namespace async {

// Must match the definition in `cxx_async/src/lib.rs`.
template <typename Future>
class RustFutureTraits {};

template <typename Future>
struct Vtable;

template <typename Future>
class FutureVtableProvider {
 public:
  static const rust::async::Vtable<Future>* vtable();
};

template <typename Future>
using RustYieldResultFor = typename RustFutureTraits<Future>::YieldResult;
template <typename Future>
using RustFinalResultFor = typename RustFutureTraits<Future>::FinalResult;
template <typename Future>
using RustChannelFor = typename RustFutureTraits<Future>::Channel;

// FIXME(pcwalton): Is making these incomplete types the right thing to do? It
// requires the macro to define drop glue for the `rust::Box` destructor to
// call, and that's a bit messy.
template <typename Future>
class RustSender;
template <typename Future>
struct RustChannel;
template <typename Future>
class RustPromiseBase;
template <typename Future, bool YieldResultIsVoid, bool FinalResultIsVoid>
class RustPromise;

class SuspendedCoroutine;

// This has to be separate from `rust::Error` because constructing a
// `rust::Error` is private API.
class Error final : public std::exception {
  char* m_message;
  void copy_from(const char* message) {
    size_t len = std::strlen(message) + 1;
    m_message = new char[len];
    std::copy(&message[0], &message[len], m_message);
  }
  void destroy() {
    if (m_message != nullptr) {
      delete[] m_message;
    }
  }
  explicit Error(const char* message) {
    copy_from(message);
  }
  template <typename Future>
  friend class RustFutureReceiver;

 public:
  Error(const Error& other) {
    copy_from(other.m_message);
  }
  Error(Error&& other) noexcept : m_message(other.m_message) {
    other.m_message = nullptr;
  }
  ~Error() noexcept override {
    destroy();
  }
  Error& operator=(const Error& other) {
    destroy();
    copy_from(other.m_message);
    return *this;
  }
  Error& operator=(Error&& other) noexcept {
    m_message = other.m_message;
    other.m_message = nullptr;
    return *this;
  }
  const char* what() const noexcept override {
    return m_message;
  }
};

// Exception customization point. This works just like
// `rust::behavior::trycatch` [1], except that it allows the `trycatch` behavior
// to declared anywhere before the future is used, reducing header file ordering
// issues.
//
// Define a specialization of the `TryCatch` with this signature in order to
// customize exception handling:
//
//      namespace behavior {
//      template<typename T>
//      struct TryCatch<T, Custom> {
//          template<typename Try, typename Fail>
//          static void trycatch(Try&& func, Fail&& fail) noexcept {
//              ...
//          }
//      };
//      } // end namespace behavior
//
// The useless-seeming `T` type parameter is required on the outer struct so
// that the name lookup inside the `Future` template will happen in the second
// phase (instantiation time) as opposed to the first phase (declaration time).
//
// This has to be separate from `rust::behavior::trycatch` because `cxx` won't
// always generate the default definition of that function, and we can't force
// it to.
//
// [1]: https://cxx.rs/binding/result.html
namespace behavior {

struct Custom {};

template <typename T, typename C>
struct TryCatch {
  template <typename Try, typename Fail>
  static void trycatch(Try&& func, Fail&& fail) noexcept {
    try {
      func();
    } catch (const std::exception& e) {
      fail(e.what());
    }
  }
};

} // namespace behavior

void cxxasync_assert(bool cond);

// Execlet API
struct RustExeclet;
extern "C" {
// Creates a new execlet.
RustExeclet* cxxasync_execlet_create();
// Decrements the reference count on an execlet and frees it if the count hits
// zero.
void cxxasync_execlet_release(RustExeclet* self);
// Submit a task to the execlet. This internally bumps the reference count.
void cxxasync_execlet_submit(RustExeclet* self, void (*run)(void*), void* task);
}

// Execlet
class Execlet {
  RustExeclet* m_priv;

  Execlet(const Execlet&) = delete;
  Execlet& operator=(const Execlet&) = delete;

 public:
  Execlet() : m_priv(cxxasync_execlet_create()) {}

  ~Execlet() {
    cxxasync_execlet_release(m_priv);
  }

  void submit(void* task, void (*run)(void*)) noexcept {
    cxxasync_execlet_submit(m_priv, run, task);
  }

  RustExeclet* raw() {
    return m_priv;
  }
};

enum class FuturePollStatus {
  Pending,
  Complete,
  Error,
  // Only used for streams, not futures.
  Running,
};

enum class FutureWakeStatus {
  Pending,
  Complete,
  Error,
  Dead,
};

// The return value of `sender_send`. These must match the `SEND_RESULT_`
// constants in `lib.rs`.
enum class RustSendResult {
  // There's no room to send a value. This task needs to go to sleep.
  Wait,
  // The value was successfully sent.
  Sent,
  // The value was successfully sent, and the channel is closed.
  Finished,
};

inline bool wake_status_is_done(FutureWakeStatus status) {
  return status == FutureWakeStatus::Complete ||
      status == FutureWakeStatus::Error;
}

// Downstream libraries can customize this by adding template specializations.
//
// A specialization should look like this:
//
//      template <typename Awaiter, typename Future>
//      class AwaitTransformer<Awaiter, Future, std::void_t</* something to
//      SFINAE */>> {
//          AwaitTransformer() = delete;
//
//         public:
//          static auto await_transform(RustPromiseBase<Future>& promise,
//          Awaiter&& awaiter)
//                  noexcept {
//              // Transform `co_await` however you'd like.
//          }
//      };
//
// See `cxx_async_folly.h` for an example.
template <typename Awaiter, typename Future, typename = void>
class AwaitTransformer {
  AwaitTransformer() = delete;

 public:
  using CantTransform = bool;
};

template <typename Future>
struct Vtable {
  RustChannel<Future> (*channel)(RustExeclet* execlet);
  uint32_t (*sender_send)(
      RustSender<Future>& self,
      uint32_t status,
      const void* value,
      const void* waker_data);
  uint32_t (*future_poll)(Future& self, void* result, const void* waker_data);
};

template <typename Future>
struct RustChannel {
  rust::Box<Future> future;
  rust::Box<RustSender<Future>> sender;
};

// A temporary place to hold future results or errors that are sent to or
// returned from Rust.
template <typename Result>
union RustFutureResult {
  Result m_result;
  rust::String m_exception;

  // When using this type, you must fill `m_result` or `m_exception` manually
  // via placement new.
  RustFutureResult() {}
  // When using this type, you must manually drop the contents.
  ~RustFutureResult() {}
};

template <>
union RustFutureResult<void> {
  rust::String m_exception;

  // When using this type, you must fill `m_exception` manually via placement
  // new.
  RustFutureResult() {}
  // When using this type, you must manually drop the contents.
  ~RustFutureResult() {}
};

template <typename Future>
class RustFutureReceiver {
  using YieldResult = typedef RustYieldResultFor<Future>;

  std::mutex m_lock;
  rust::Box<Future> m_future;
  RustFutureResult<YieldResult> m_result;
  FuturePollStatus m_status;

  RustFutureReceiver(const RustFutureReceiver&) = delete;
  void operator=(const RustFutureReceiver&) = delete;

 public:
  explicit RustFutureReceiver(rust::Box<Future>&& future)
      : m_lock(),
        m_future(std::move(future)),
        m_status(FuturePollStatus::Pending) {}

  // Consumes the `coroutine` reference (so you probably want to addref it
  // first).
  FutureWakeStatus wake(SuspendedCoroutine* coroutine);

  YieldResult&& get_result() {
    // Safe to use without taking the lock because the caller asserts that the
    // future has already completed.
    switch (m_status) {
      case FuturePollStatus::Complete:
        return std::move(m_result.m_result);
      case FuturePollStatus::Error:
        throw Error(m_result.m_exception.c_str());
      case FuturePollStatus::Pending:
      case FuturePollStatus::Running:
        // TODO(pcwalton): Handle C++ consuming Rust streams.
        CXXASYNC_ASSERT(false);
        std::terminate();
    }
  }
};

template <typename Future>
class RustAwaiter {
  using YieldResult = RustYieldResultFor<Future>;

  friend class SuspendedCoroutine;

  std::shared_ptr<RustFutureReceiver<Future>> m_receiver;

  RustAwaiter(const RustAwaiter&) = delete;
  void operator=(const RustAwaiter&) = delete;

 public:
  explicit RustAwaiter(rust::Box<Future>&& future)
      : m_receiver(
            std::make_shared<RustFutureReceiver<Future>>(std::move(future))) {}

  bool await_ready() noexcept {
    // We could poll here, but let's not. Assume that polling is more expensive
    // than creating the coroutine state.
    return false;
  }

  bool await_suspend(std::experimental::coroutine_handle<void> next);

  YieldResult&& await_resume() {
    return m_receiver->get_result();
  }
};

template <typename Future>
class RustStreamAwaiter {
  using YieldResult = RustYieldResultFor<Future>;

  friend class SuspendedCoroutine;

  // FIXME(pcwalton): I think this needs to be a `shared_ptr`!
  RustSender<Future>& m_sender;
  YieldResult&& m_value;

  FutureWakeStatus poll_next(SuspendedCoroutine* coroutine) noexcept;

  RustStreamAwaiter(const RustStreamAwaiter&) = delete;
  void operator=(const RustStreamAwaiter&) = delete;

 public:
  RustStreamAwaiter(RustSender<Future>& sender, YieldResult&& value)
      : m_sender(sender), m_value(std::move(value)) {}

  bool await_ready() noexcept {
    return false;
  }
  bool await_suspend(std::experimental::coroutine_handle<void> next);
  void await_resume() {}
};

// This is like `std::experimental::coroutine_handle<void>`, but it doesn't
// *have* to be a coroutine handle.
class Continuation {
  Continuation(const Continuation&) = delete;
  void operator=(const Continuation&) = delete;

 protected:
  Continuation() {}

 public:
  virtual ~Continuation() {}
  virtual void resume() = 0;
  virtual void destroy() = 0;
};

class CoroutineHandleContinuation : public Continuation {
  std::experimental::coroutine_handle<void> m_next;

 public:
  explicit CoroutineHandleContinuation(std::experimental::coroutine_handle<void>&& next)
      : m_next(next) {}
  void resume() override {
    m_next.resume();
  }
  void destroy() override {
    m_next.destroy();
  }
};

// Wrapper object that encapsulates a suspended coroutine. This is the waker
// that is exposed to Rust.
//
// This object is *manually* reference counted via `add_ref()` and `release()`,
// to match the `RawWaker` interface that Rust expects.
class SuspendedCoroutine {
  SuspendedCoroutine(const SuspendedCoroutine&) = delete;
  void operator=(const SuspendedCoroutine&) = delete;

  typedef std::function<FutureWakeStatus(SuspendedCoroutine*)> WakeFn;

  std::atomic<uintptr_t> m_refcount;
  std::unique_ptr<Continuation> m_next;
  WakeFn m_wake_fn;

  void forget_coroutine_handle() {
    m_next.reset();
  }

 public:
  SuspendedCoroutine(std::unique_ptr<Continuation>&& next, WakeFn&& wake_fn)
      : m_refcount(1), m_next(std::move(next)), m_wake_fn(std::move(wake_fn)) {}

  ~SuspendedCoroutine() {
    if (m_next) {
      m_next->destroy();
      m_next.reset();
    }
  }

  SuspendedCoroutine* add_ref() {
    m_refcount.fetch_add(1);
    return this;
  }

  void release() {
    uintptr_t last_refcount = m_refcount.fetch_sub(1);
    CXXASYNC_ASSERT(last_refcount > 0);
    if (last_refcount == 1) {
      delete this;
    }
  }

  // Does not consume the `this` reference.
  FutureWakeStatus wake() {
    return m_wake_fn(this);
  }

  // Performs the initial poll needed when we go to sleep for the first time.
  // Returns true if we should go to sleep and false otherwise.
  //
  // Consumes the `this` reference.
  bool initial_suspend() {
    FutureWakeStatus status = wake();

    // Tricky: if the future is already complete, we won't go to sleep, which
    // means we won't resume, so unless we intervene like this nothing will stop
    // our destructor from destroying the coroutine handle.
    bool done = wake_status_is_done(status);
    if (done) {
      forget_coroutine_handle();
    }
    release();
    return !done;
  }

  void resume() {
    CXXASYNC_ASSERT(bool(m_next));
    std::unique_ptr<Continuation> next = std::move(m_next);
    forget_coroutine_handle();
    next->resume();
  }
};

// Promise object that manages the channel that is returned to Rust when Rust
// calls a C++ coroutine.
template <typename Future>
class RustPromiseBase {
  using Channel = RustChannel<Future>;

  // This must precede `m_channel`.
  Execlet m_execlet;

  RustPromiseBase(const RustPromiseBase&) = delete;
  RustPromiseBase& operator=(const RustPromiseBase&) = delete;

 protected:
  Channel m_channel;

 public:
  RustPromiseBase()
      : m_execlet(),
        m_channel(
            FutureVtableProvider<Future>::vtable()->channel(m_execlet.raw())) {}

  rust::Box<Future> get_return_object() noexcept {
    return std::move(m_channel.future);
  }

  std::experimental::suspend_never initial_suspend() const noexcept {
    return {};
  }
  std::experimental::suspend_never final_suspend() const noexcept {
    return {};
  }
  std::experimental::coroutine_handle<> unhandled_done() noexcept {
    return {};
  }

  void unhandled_exception() noexcept {
    behavior::TryCatch<Future, behavior::Custom>::trycatch(
        []() { std::rethrow_exception(std::current_exception()); },
        [&](const char* what) {
          const Vtable<Future>* vtable = FutureVtableProvider<Future>::vtable();
          vtable->sender_send(
              *m_channel.sender,
              static_cast<uint32_t>(FuturePollStatus::Error),
              what,
              nullptr);
        });
  }

  Execlet& execlet() noexcept {
    return m_execlet;
  }

  // Customization point for library integration (e.g. Folly).
  template <
      typename Awaiter,
      decltype(AwaitTransformer<Awaiter, Future>::await_transform(
          *static_cast<RustPromiseBase<Future>*>(nullptr),
          std::declval<Awaiter>()))* = nullptr>
  auto await_transform(Awaiter&& awaitable) noexcept {
    return AwaitTransformer<Awaiter, Future>::await_transform(
        *this, std::forward<Awaiter>(awaitable));
  }
  // Default implementation of the above if the library doesn't need to
  // customize `co_await`.
  template <
      typename Awaiter,
      typename AwaitTransformer<Awaiter, Future>::CantTransform = true>
  auto&& await_transform(Awaiter&& awaitable) noexcept {
    return std::forward<Awaiter>(awaitable);
  }
};

// FIXME(pcwalton): Boy, this class hierarchy is ugly.
template <typename Future, bool YieldResultIsVoid>
class RustStreamPromiseBase : public RustPromiseBase<Future> {};

// Non-void specialization.
template <typename Future>
class RustStreamPromiseBase<Future, false> : public RustPromiseBase<Future> {
  using YieldResult = RustYieldResultFor<Future>;

 public:
  RustStreamAwaiter<Future> yield_value(
      RustYieldResultFor<Future>&& value) noexcept {
    return RustStreamAwaiter(*this->m_channel.sender, std::move(value));
  }
};

// Void specialization.
template <typename Future>
class RustStreamPromiseBase<Future, true> : public RustPromiseBase<Future> {};

template <
    typename Future,
    bool YieldResultIsVoid =
        std::is_same<RustYieldResultFor<Future>, void>::value,
    bool FinalResultIsVoid =
        std::is_same<RustFinalResultFor<Future>, void>::value>
class RustPromise final
    : public RustStreamPromiseBase<Future, YieldResultIsVoid> {};

// Non-void specialization.
template <typename Future, bool YieldResultIsVoid>
class RustPromise<Future, YieldResultIsVoid, false> final
    : public RustStreamPromiseBase<Future, YieldResultIsVoid> {
  using FinalResult = RustFinalResultFor<Future>;

 public:
  void return_value(FinalResult&& value) {
    RustFutureResult<FinalResult> result;
    new (&result.m_result) FinalResult(std::move(value));
    FutureVtableProvider<Future>::vtable()->sender_send(
        *this->m_channel.sender,
        static_cast<uint32_t>(FuturePollStatus::Complete),
        reinterpret_cast<const uint8_t*>(&result),
        nullptr);
  }
};

// Void specialization.
template <typename Future, bool YieldResultIsVoid>
class RustPromise<Future, YieldResultIsVoid, true> final
    : public RustStreamPromiseBase<Future, YieldResultIsVoid> {
 public:
  void return_void() {
    FutureVtableProvider<Future>::vtable()->sender_send(
        *this->m_channel.sender,
        static_cast<uint32_t>(FuturePollStatus::Complete),
        nullptr,
        nullptr);
  }
};

// Consumes the `coroutine` reference (so you probably want to addref it first).
template <typename Future>
FutureWakeStatus RustFutureReceiver<Future>::wake(
    SuspendedCoroutine* coroutine) {
  std::lock_guard<std::mutex> guard(m_lock);

  // Have we already polled this future to completion? If so, don't poll again.
  if (m_status != FuturePollStatus::Pending) {
    coroutine->release();
    return FutureWakeStatus::Dead;
  }

  m_status = static_cast<FuturePollStatus>(
      FutureVtableProvider<Future>::vtable()->future_poll(
          *m_future, &m_result, coroutine));
  return static_cast<FutureWakeStatus>(m_status);
}

template <typename Future>
inline bool RustAwaiter<Future>::await_suspend(
    std::experimental::coroutine_handle<void> next) {
  std::weak_ptr<RustFutureReceiver<Future>> weak_receiver = m_receiver;
  SuspendedCoroutine* coroutine = new SuspendedCoroutine(
      std::make_unique<CoroutineHandleContinuation>(std::move(next)),
      [weak_receiver =
           std::move(weak_receiver)](SuspendedCoroutine* coroutine) {
        std::shared_ptr<RustFutureReceiver<Future>> receiver =
            weak_receiver.lock();
        // This rarely ever happens in practice, but I think it can.
        if (!receiver) {
          return FutureWakeStatus::Dead;
        }
        return receiver->wake(coroutine->add_ref());
      });
  return coroutine->initial_suspend();
}

template <typename Future>
inline bool RustStreamAwaiter<Future>::await_suspend(
    std::experimental::coroutine_handle<void> next) {
  SuspendedCoroutine* coroutine = new SuspendedCoroutine(
      std::make_unique<CoroutineHandleContinuation>(std::move(next)),
      [=](SuspendedCoroutine* coroutine) {
        return this->poll_next(coroutine);
      });
  return coroutine->initial_suspend();
}

template <typename Future>
inline FutureWakeStatus RustStreamAwaiter<Future>::poll_next(
    SuspendedCoroutine* coroutine) noexcept {
  RustFutureResult<YieldResult> result;
  new (&result.m_result) YieldResult(std::move(m_value));
  RustSendResult send_result = static_cast<RustSendResult>(
      FutureVtableProvider<Future>::vtable()->sender_send(
          this->m_sender,
          static_cast<uint32_t>(FuturePollStatus::Running),
          reinterpret_cast<const uint8_t*>(&result),
          coroutine->add_ref()));

  switch (send_result) {
    case RustSendResult::Sent:
      return FutureWakeStatus::Complete;
    case RustSendResult::Wait:
      m_value = std::move(result.m_result);
      return FutureWakeStatus::Pending;
    case RustSendResult::Finished:
      // Should never get here.
      CXXASYNC_ASSERT(false);
      std::terminate();
  }
}

} // namespace async

// This can't be in the `rust::async` namespace because it relies on ADL on
// `rust::Box<Future>` to be found.
template <typename Future>
inline async::RustAwaiter<Future> operator co_await(
    Box<Future>&& future) noexcept {
  return async::RustAwaiter(std::move(future));
}

} // namespace rust

template <typename Future, typename... Args>
struct std::experimental::coroutine_traits<rust::Box<Future>, Args...> {
  using promise_type = rust::async::RustPromise<Future>;
};

#endif // RUST_CXX_ASYNC_H
