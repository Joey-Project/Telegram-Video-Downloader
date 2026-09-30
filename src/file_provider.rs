use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;

pub(crate) trait QueueFileProvider: Send + Sync {
    fn coordinate_read(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()>;

    fn coordinate_write(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()>;

    fn coordinate_move(
        &self,
        source: &Path,
        destination: &Path,
        accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
    ) -> Result<()>;
}

#[derive(Debug)]
pub(crate) struct FileProviderAccessError {
    path: PathBuf,
    operation: &'static str,
    detail: String,
}

impl FileProviderAccessError {
    pub(crate) fn new(path: &Path, operation: &'static str, detail: impl Into<String>) -> Self {
        Self {
            path: path.to_path_buf(),
            operation,
            detail: detail.into(),
        }
    }
}

impl Display for FileProviderAccessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "macOS could not coordinate the {} of task queue file {}: {}",
            self.operation,
            self.path.display(),
            self.detail
        )
    }
}

impl Error for FileProviderAccessError {}

pub(crate) fn is_file_provider_access_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.downcast_ref::<FileProviderAccessError>().is_some())
}

pub(crate) fn is_deadlock_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.raw_os_error() == Some(libc::EDEADLK))
    })
}

pub(crate) fn classify_deadlock_error(
    path: &Path,
    operation: &'static str,
    error: anyhow::Error,
) -> anyhow::Error {
    if is_deadlock_error(&error) {
        FileProviderAccessError::new(path, operation, format!("{error:#}")).into()
    } else {
        error
    }
}

pub(crate) fn platform_queue_file_provider() -> Arc<dyn QueueFileProvider> {
    #[cfg(target_os = "macos")]
    {
        Arc::new(MacQueueFileProvider)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Arc::new(PassthroughQueueFileProvider)
    }
}

#[cfg(not(target_os = "macos"))]
struct PassthroughQueueFileProvider;

#[cfg(not(target_os = "macos"))]
impl QueueFileProvider for PassthroughQueueFileProvider {
    fn coordinate_read(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        accessor(path)
    }

    fn coordinate_write(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        accessor(path)
    }

    fn coordinate_move(
        &self,
        source: &Path,
        destination: &Path,
        accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
    ) -> Result<()> {
        accessor(source, destination)
    }
}

#[cfg(target_os = "macos")]
struct MacQueueFileProvider;

#[cfg(target_os = "macos")]
impl QueueFileProvider for MacQueueFileProvider {
    fn coordinate_read(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        coordinate(path, false, accessor)
    }

    fn coordinate_write(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        coordinate(path, true, accessor)
    }

    fn coordinate_move(
        &self,
        source: &Path,
        destination: &Path,
        accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
    ) -> Result<()> {
        coordinate_move(source, destination, accessor)
    }
}

#[cfg(target_os = "macos")]
fn coordinate(
    path: &Path,
    write: bool,
    accessor: &mut dyn FnMut(&Path) -> Result<()>,
) -> Result<()> {
    use std::cell::RefCell;
    use std::ptr::NonNull;

    use block2::StackBlock;
    use objc2::rc::Retained;
    use objc2_foundation::{
        NSError, NSFileCoordinator, NSFileCoordinatorReadingOptions,
        NSFileCoordinatorWritingOptions, NSURL,
    };

    let url = NSURL::from_file_path(path).ok_or_else(|| {
        FileProviderAccessError::new(path, operation_name(write), "path is not a valid file URL")
    })?;
    let coordinator = NSFileCoordinator::new();
    let accessor_result = RefCell::new(None);
    let accessor = RefCell::new(accessor);
    let block = StackBlock::new(|coordinated_url: NonNull<NSURL>| {
        let coordinated_path = unsafe { coordinated_url.as_ref() }.to_file_path();
        let result = match coordinated_path {
            Some(coordinated_path) => (accessor.borrow_mut())(&coordinated_path),
            None => Err(FileProviderAccessError::new(
                path,
                operation_name(write),
                "File Provider returned a non-file URL",
            )
            .into()),
        };
        accessor_result.replace(Some(result));
    });
    let mut coordination_error: Option<Retained<NSError>> = None;

    if write {
        coordinator.coordinateWritingItemAtURL_options_error_byAccessor(
            &url,
            NSFileCoordinatorWritingOptions::ForReplacing,
            Some(&mut coordination_error),
            &block,
        );
    } else {
        coordinator.coordinateReadingItemAtURL_options_error_byAccessor(
            &url,
            NSFileCoordinatorReadingOptions::empty(),
            Some(&mut coordination_error),
            &block,
        );
    }

    if let Some(error) = coordination_error {
        return Err(FileProviderAccessError::new(
            path,
            operation_name(write),
            format!("{error:?}"),
        )
        .into());
    }

    accessor_result.into_inner().ok_or_else(|| {
        FileProviderAccessError::new(
            path,
            operation_name(write),
            "the coordinated accessor was not called",
        )
    })?
}

#[cfg(target_os = "macos")]
fn coordinate_move(
    source: &Path,
    destination: &Path,
    accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
) -> Result<()> {
    use std::cell::RefCell;
    use std::ptr::NonNull;

    use block2::StackBlock;
    use objc2::rc::Retained;
    use objc2_foundation::{NSError, NSFileCoordinator, NSFileCoordinatorWritingOptions, NSURL};

    let source_url = NSURL::from_file_path(source).ok_or_else(|| {
        FileProviderAccessError::new(source, "move", "source is not a valid file URL")
    })?;
    let destination_url = NSURL::from_file_path(destination).ok_or_else(|| {
        FileProviderAccessError::new(destination, "move", "destination is not a valid file URL")
    })?;
    let coordinator = NSFileCoordinator::new();
    let accessor_result = RefCell::new(None);
    let accessor = RefCell::new(accessor);
    let block = StackBlock::new(
        |coordinated_source: NonNull<NSURL>, coordinated_destination: NonNull<NSURL>| {
            let source_path = unsafe { coordinated_source.as_ref() }.to_file_path();
            let destination_path = unsafe { coordinated_destination.as_ref() }.to_file_path();
            let result = match (source_path, destination_path) {
                (Some(source_path), Some(destination_path)) => {
                    (accessor.borrow_mut())(&source_path, &destination_path)
                }
                _ => Err(FileProviderAccessError::new(
                    source,
                    "move",
                    "File Provider returned a non-file URL",
                )
                .into()),
            };
            accessor_result.replace(Some(result));
        },
    );
    let mut coordination_error: Option<Retained<NSError>> = None;

    coordinator.coordinateWritingItemAtURL_options_writingItemAtURL_options_error_byAccessor(
        &source_url,
        NSFileCoordinatorWritingOptions::ForMoving,
        &destination_url,
        NSFileCoordinatorWritingOptions::ForReplacing,
        Some(&mut coordination_error),
        &block,
    );

    if let Some(error) = coordination_error {
        return Err(FileProviderAccessError::new(source, "move", format!("{error:?}")).into());
    }

    accessor_result.into_inner().ok_or_else(|| {
        FileProviderAccessError::new(source, "move", "the coordinated accessor was not called")
    })?
}

#[cfg(target_os = "macos")]
fn operation_name(write: bool) -> &'static str {
    if write { "write" } else { "read" }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct MockQueueFileProvider {
    reads: std::sync::Mutex<Vec<PathBuf>>,
    writes: std::sync::Mutex<Vec<PathBuf>>,
    moves: std::sync::Mutex<Vec<(PathBuf, PathBuf)>>,
    accessor_paths: std::sync::Mutex<Vec<PathBuf>>,
    path_rewrites: std::sync::Mutex<Vec<(PathBuf, PathBuf)>>,
    next_read_failure: std::sync::Mutex<Option<String>>,
    next_write_failure: std::sync::Mutex<Option<String>>,
    next_move_failure: std::sync::Mutex<Option<String>>,
    write_failure_after: std::sync::Mutex<Option<(usize, String)>>,
    #[cfg(unix)]
    read_path_replacement: std::sync::Mutex<Option<(PathBuf, PathBuf)>>,
}

#[cfg(test)]
impl MockQueueFileProvider {
    pub(crate) fn fail_next_read(&self, detail: impl Into<String>) {
        *self
            .next_read_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(detail.into());
    }

    pub(crate) fn read_paths(&self) -> Vec<PathBuf> {
        self.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn write_paths(&self) -> Vec<PathBuf> {
        self.writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn move_paths(&self) -> Vec<(PathBuf, PathBuf)> {
        self.moves
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn rewrite_paths_under(&self, from: &Path, to: &Path) {
        self.path_rewrites
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((from.to_path_buf(), to.to_path_buf()));
    }

    #[cfg(unix)]
    pub(crate) fn replace_symlink_after_next_read(&self, path: &Path, target: &Path) {
        *self
            .read_path_replacement
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((path.to_path_buf(), target.to_path_buf()));
    }

    pub(crate) fn accessor_paths(&self) -> Vec<PathBuf> {
        self.accessor_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn fail_next_write(&self, detail: impl Into<String>) {
        *self
            .next_write_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(detail.into());
    }

    pub(crate) fn fail_next_move(&self, detail: impl Into<String>) {
        *self
            .next_move_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(detail.into());
    }

    pub(crate) fn fail_write_after(&self, successful_writes: usize, detail: impl Into<String>) {
        *self
            .write_failure_after
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((successful_writes, detail.into()));
    }

    pub(crate) fn fail_next_file_provider_write(&self, detail: impl Into<String>) {
        self.fail_write_after(0, detail);
    }

    fn accessor_path(&self, path: &Path) -> PathBuf {
        self.path_rewrites
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find_map(|(from, to)| {
                path.strip_prefix(from)
                    .ok()
                    .map(|relative| to.join(relative))
            })
            .unwrap_or_else(|| path.to_path_buf())
    }
}

#[cfg(test)]
impl QueueFileProvider for MockQueueFileProvider {
    fn coordinate_read(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        self.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(path.to_path_buf());
        if let Some(detail) = self
            .next_read_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            return Err(FileProviderAccessError::new(path, "read", detail).into());
        }
        let accessor_path = self.accessor_path(path);
        self.accessor_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(accessor_path.clone());
        let result = accessor(&accessor_path);
        #[cfg(unix)]
        {
            let replacement = {
                let mut replacement = self
                    .read_path_replacement
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                replacement
                    .as_ref()
                    .is_some_and(|(expected, _)| expected == path)
                    .then(|| replacement.take())
                    .flatten()
            };
            if let Some((path, target)) = replacement {
                std::fs::remove_file(&path)?;
                std::os::unix::fs::symlink(target, path)?;
            }
        }
        result
    }

    fn coordinate_write(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        self.writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(path.to_path_buf());
        if let Some(detail) = self
            .next_write_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            return Err(anyhow::anyhow!(detail));
        }
        let delayed_failure = {
            let mut failure = self
                .write_failure_after
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match failure.as_mut() {
                Some((remaining, _)) if *remaining == 0 => failure.take().map(|(_, detail)| detail),
                Some((remaining, _)) => {
                    *remaining -= 1;
                    None
                }
                None => None,
            }
        };
        if let Some(detail) = delayed_failure {
            return Err(FileProviderAccessError::new(path, "write", detail).into());
        }
        let accessor_path = self.accessor_path(path);
        self.accessor_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(accessor_path.clone());
        accessor(&accessor_path)
    }

    fn coordinate_move(
        &self,
        source: &Path,
        destination: &Path,
        accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
    ) -> Result<()> {
        self.moves
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((source.to_path_buf(), destination.to_path_buf()));
        if let Some(detail) = self
            .next_move_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            return Err(FileProviderAccessError::new(source, "move", detail).into());
        }
        let accessor_source = self.accessor_path(source);
        let accessor_destination = self.accessor_path(destination);
        self.accessor_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend([accessor_source.clone(), accessor_destination.clone()]);
        accessor(&accessor_source, &accessor_destination)
    }
}
