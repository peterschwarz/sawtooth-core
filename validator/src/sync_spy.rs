use std::ops::{Deref, DerefMut};
use std::sync::{LockResult, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Default)]
pub struct SpyRwLock<T> {
    name: String,
    lock: RwLock<T>,
}
impl<T> SpyRwLock<T> {
    pub fn new(name: &str, t: T) -> Self {
        SpyRwLock {
            name: name.into(),
            lock: RwLock::new(t),
        }
    }

    pub fn read(&self) -> LockResult<SpyRwLockReadGuard<T>> {
        eprintln!(
            "{} [{:?}]: Locking Read",
            self.name,
            ::std::thread::current().name()
        );
        let guard = self.lock
            .read()
            .map(|guard| {
                SpyRwLockReadGuard::new(
                    &format!("{} [{:?}]", self.name, ::std::thread::current()),
                    guard,
                )
            })
            .map_err(|err| PoisonError::new(SpyRwLockReadGuard::new(&self.name, err.into_inner())));
        eprintln!(
            "{} [{:?}]: Locked Read",
            self.name,
            ::std::thread::current().name()
        );
        guard
    }

    pub fn read_with_meta(&self, file: &str, line: u32) -> LockResult<SpyRwLockReadGuard<T>> {
        eprintln!(
            "{} [{}: {}, {:?}]: Locking Read",
            self.name,
            file,
            line,
            ::std::thread::current().name()
        );
        let guard = self.lock
            .read()
            .map(|guard| {
                SpyRwLockReadGuard::new(
                    &format!(
                        "{} [{}: {}, {:?}]",
                        self.name,
                        file,
                        line,
                        ::std::thread::current()
                    ),
                    guard,
                )
            })
            .map_err(|err| PoisonError::new(SpyRwLockReadGuard::new(&self.name, err.into_inner())));

        eprintln!(
            "{} [{}: {}, {:?}]: Locked Read",
            self.name,
            file,
            line,
            ::std::thread::current().name()
        );

        guard
    }

    pub fn write(&self) -> LockResult<SpyRwLockWriteGuard<T>> {
        eprintln!(
            "{} [{:?}]: Locking Write",
            self.name,
            ::std::thread::current().name()
        );
        let guard = self.lock
            .write()
            .map(|guard| SpyRwLockWriteGuard::new(&self.name, guard))
            .map_err(|err| {
                PoisonError::new(SpyRwLockWriteGuard::new(
                    &format!("{} [{:?}]", self.name, ::std::thread::current().name()),
                    err.into_inner(),
                ))
            });
        eprintln!(
            "{} [{:?}]: Locked Write",
            self.name,
            ::std::thread::current().name()
        );
        guard
    }

    pub fn write_with_meta(&self, file: &str, line: u32) -> LockResult<SpyRwLockWriteGuard<T>> {
        eprintln!(
            "{} [{}: {}, {:?}]: Locking Write",
            self.name,
            file,
            line,
            ::std::thread::current().name()
        );
        let guard = self.lock
            .write()
            .map(|guard| {
                SpyRwLockWriteGuard::new(
                    &format!(
                        "{} [{}: {}, {:?}]",
                        self.name,
                        file,
                        line,
                        ::std::thread::current()
                    ),
                    guard,
                )
            })
            .map_err(|err| {
                PoisonError::new(SpyRwLockWriteGuard::new(&self.name, err.into_inner()))
            });

        eprintln!(
            "{} [{}: {}, {:?}]: Locked Write",
            self.name,
            file,
            line,
            ::std::thread::current().name()
        );
        guard
    }
}

pub struct SpyRwLockReadGuard<'a, T: ?Sized + 'a> {
    name: String,
    guard: RwLockReadGuard<'a, T>,
}

impl<'a, T: ?Sized + 'a> SpyRwLockReadGuard<'a, T> {
    fn new(name: &str, guard: RwLockReadGuard<'a, T>) -> Self {
        SpyRwLockReadGuard {
            name: name.to_string(),
            guard,
        }
    }
}

impl<'a, T: ?Sized + 'a> Deref for SpyRwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.guard.deref()
    }
}

impl<'a, T: ?Sized + 'a> Drop for SpyRwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        eprintln!("{}: Releasing Read", self.name);
    }
}

pub struct SpyRwLockWriteGuard<'a, T: ?Sized + 'a> {
    name: String,
    guard: RwLockWriteGuard<'a, T>,
}

impl<'a, T: ?Sized + 'a> SpyRwLockWriteGuard<'a, T> {
    fn new(name: &str, guard: RwLockWriteGuard<'a, T>) -> Self {
        SpyRwLockWriteGuard {
            name: name.to_string(),
            guard,
        }
    }
}

impl<'a, T: ?Sized + 'a> Deref for SpyRwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.guard.deref()
    }
}

impl<'a, T: ?Sized + 'a> DerefMut for SpyRwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.guard.deref_mut()
    }
}

impl<'a, T: ?Sized + 'a> Drop for SpyRwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        eprintln!("{}: Releasing Write", self.name);
    }
}
