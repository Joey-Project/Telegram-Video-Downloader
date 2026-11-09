#![cfg(target_os = "macos")]

use std::cell::{Cell, RefCell};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use objc2::rc::{Retained, Weak, autoreleasepool};
use objc2::runtime::NSObject;

use super::{MacQueueFileProvider, QueueFileProvider};

static TEMP_DIRECTORY_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempTree {
    path: PathBuf,
}

impl TempTree {
    fn new(label: &str) -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after the Unix epoch")
            .as_nanos();
        let counter = TEMP_DIRECTORY_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "telegram-video-downloader-file-provider-{label}-{}-{stamp:x}-{counter:x}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("temporary test directory should be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[derive(Default)]
struct AutoreleasedObjectObserver {
    weak: RefCell<Option<Weak<NSObject>>>,
}

impl AutoreleasedObjectObserver {
    fn create_autoreleased_object(&self) {
        // `autorelease_ptr` puts the +1 reference into the currently innermost
        // Objective-C pool. The weak reference lets the test observe when that
        // pool releases the object without retaining it itself.
        let object = Retained::autorelease_ptr(NSObject::new());
        // SAFETY: `object` remains alive in the current autorelease pool while
        // the weak observer is initialized.
        let weak = unsafe { Weak::new(&*object) };
        assert!(
            self.weak.borrow_mut().replace(weak).is_none(),
            "each operation should receive a fresh observer"
        );
    }

    fn is_alive(&self) -> bool {
        self.weak
            .borrow()
            .as_ref()
            .expect("the accessor should create an Objective-C sentinel")
            .load()
            .is_some()
    }

    fn assert_drained(&self, operation: &str) {
        assert!(
            !self.is_alive(),
            "the autoreleased Objective-C object survived the {operation} operation"
        );
    }
}

fn on_long_lived_rust_thread(run: impl FnOnce() + Send + 'static) {
    thread::Builder::new()
        .name("file-provider-autorelease-test".to_string())
        .spawn(move || {
            // This is the thread's fallback pool. Without a pool in each real
            // File Provider operation, callback objects remain alive here
            // until the whole long-lived thread scope ends.
            autoreleasepool(|_| run());
        })
        .expect("test thread should start")
        .join()
        .expect("test thread should finish without panicking");
}

#[test]
fn coordinated_read_drains_callback_autoreleases() {
    on_long_lived_rust_thread(|| {
        let tree = TempTree::new("read");
        let provider = MacQueueFileProvider;
        let read_path = tree.path().join("read.txt");
        fs::write(&read_path, b"read fixture").expect("read fixture should be created");
        let read_observer = AutoreleasedObjectObserver::default();
        let mut read_accessor = |coordinated_path: &Path| {
            assert!(coordinated_path.is_file());
            read_observer.create_autoreleased_object();
            Ok(())
        };
        provider
            .coordinate_read(&read_path, &mut read_accessor)
            .expect("local coordinated read should succeed");
        read_observer.assert_drained("read");
    });
}

#[test]
fn coordinated_write_drains_callback_autoreleases() {
    on_long_lived_rust_thread(|| {
        let tree = TempTree::new("write");
        let provider = MacQueueFileProvider;
        let write_path = tree.path().join("write.txt");
        fs::write(&write_path, b"before").expect("write fixture should be created");
        let write_observer = AutoreleasedObjectObserver::default();
        let mut write_accessor = |coordinated_path: &Path| {
            write_observer.create_autoreleased_object();
            fs::write(coordinated_path, b"after")?;
            Ok(())
        };
        provider
            .coordinate_write(&write_path, &mut write_accessor)
            .expect("local coordinated write should succeed");
        write_observer.assert_drained("write");
        assert_eq!(fs::read(&write_path).unwrap(), b"after");
    });
}

#[test]
fn coordinated_move_drains_callback_autoreleases() {
    on_long_lived_rust_thread(|| {
        let tree = TempTree::new("move");
        let provider = MacQueueFileProvider;
        let move_source = tree.path().join("move-source.txt");
        let move_destination = tree.path().join("move-destination.txt");
        fs::write(&move_source, b"move fixture").expect("move fixture should be created");
        let move_observer = AutoreleasedObjectObserver::default();
        let mut move_accessor = |coordinated_source: &Path, coordinated_destination: &Path| {
            move_observer.create_autoreleased_object();
            fs::rename(coordinated_source, coordinated_destination)?;
            Ok(())
        };
        provider
            .coordinate_move(&move_source, &move_destination, &mut move_accessor)
            .expect("local coordinated move should succeed");
        move_observer.assert_drained("move");
        assert!(!move_source.exists());
        assert_eq!(fs::read(&move_destination).unwrap(), b"move fixture");
    });
}

#[test]
fn accessor_error_still_drains_callback_autoreleases() {
    on_long_lived_rust_thread(|| {
        let tree = TempTree::new("accessor-error");
        let path = tree.path().join("record.json");
        fs::write(&path, b"{}").expect("local fixture should be created");
        let observer = AutoreleasedObjectObserver::default();
        let mut accessor = |_coordinated_path: &Path| {
            observer.create_autoreleased_object();
            Err(anyhow!("intentional accessor failure"))
        };

        let error = MacQueueFileProvider.coordinate_read(&path, &mut accessor);
        assert!(error.is_err(), "the accessor failure should be returned");
        observer.assert_drained("failed read");
    });
}

#[test]
fn nested_coordination_drains_the_inner_pool_before_the_outer_pool() {
    on_long_lived_rust_thread(|| {
        let tree = TempTree::new("nested");
        let outer_path = tree.path().join("outer.txt");
        let inner_path = tree.path().join("inner.txt");
        fs::write(&outer_path, b"outer").expect("outer fixture should be created");
        fs::write(&inner_path, b"inner").expect("inner fixture should be created");

        let provider = MacQueueFileProvider;
        let outer_observer = AutoreleasedObjectObserver::default();
        let inner_observer = AutoreleasedObjectObserver::default();
        let inner_drained_before_outer_return = Cell::new(false);
        let outer_alive_after_inner_return = Cell::new(false);
        let mut outer_accessor = |_outer_coordinated_path: &Path| {
            outer_observer.create_autoreleased_object();
            let mut inner_accessor = |_inner_coordinated_path: &Path| {
                inner_observer.create_autoreleased_object();
                Ok(())
            };
            provider.coordinate_read(&inner_path, &mut inner_accessor)?;

            inner_drained_before_outer_return.set(!inner_observer.is_alive());
            outer_alive_after_inner_return.set(outer_observer.is_alive());
            Ok(())
        };

        provider
            .coordinate_read(&outer_path, &mut outer_accessor)
            .expect("nested local coordinated reads should succeed");
        assert!(
            inner_drained_before_outer_return.get(),
            "the nested operation's autorelease pool should drain before the outer operation returns"
        );
        assert!(
            outer_alive_after_inner_return.get(),
            "the inner pool must not drain the outer operation's object"
        );
        outer_observer.assert_drained("outer read");
    });
}
