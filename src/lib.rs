//! # Single Binary file Version Control system
//!
//! This crate is backend for SBVC that provides useful and simple API to use
//! in the frontend.
//!
//! ## Get Started
//!
//! To get starting using the API, refer to documentations of following
//! `struct`s:
//!
//! * [`Sbvc`]
//! * [`Version`]
//!
//! [`Sbvc`]: Versions
//! [`Version`]: Version

#![deny(missing_docs)]

use std::{
    error::Error,
    ffi::OsStr,
    fmt,
    fmt::{Display, Formatter},
    fs, io,
    num::ParseIntError,
    os::unix::prelude::OsStrExt,
    path::{Path, PathBuf},
    str::{self, Utf8Error},
    time::{Duration, SystemTime},
};

use nelf::{NelfIter, ToCell};
use wgdiff::{
    Deletion, Diff, Difference, OwnedDifference, OwnedInsertion, Patch,
};

const INIT_VERSION_NAME: &str = "init";
const DEFAULT_VERSION_NAME: &str = "unnamed";

/// An enum that represents any error that can occur while using this library.
#[derive(Debug)]
pub enum SbvcError {
    /// An IO error.
    Io(io::Error),
    /// Invalid version tree file format error.
    ///
    /// Occurs when something is missing from the version tree file. Contains
    /// a string describing the error.
    InvalidFormat(String),
    /// UTF-8 error.
    Utf8(Utf8Error),
    /// Integer parse error.
    Parse(ParseIntError),
    /// Version not found error.
    ///
    /// Contains the index of the version that was not found.
    VersionNotFound(u32),
}

impl From<io::Error> for SbvcError {
    fn from(error: io::Error) -> Self {
        SbvcError::Io(error)
    }
}

impl From<Utf8Error> for SbvcError {
    fn from(error: Utf8Error) -> Self {
        SbvcError::Utf8(error)
    }
}

impl From<ParseIntError> for SbvcError {
    fn from(error: ParseIntError) -> Self {
        SbvcError::Parse(error)
    }
}

impl Display for SbvcError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            SbvcError::Io(error) => write!(f, "SBVC IO Error: {}", error),
            SbvcError::InvalidFormat(error) => {
                write!(f, "SBVC Invalid Format Error: {}", error)
            }
            SbvcError::Utf8(error) => write!(f, "SBVC UTF-8 Error: {}", error),
            SbvcError::Parse(error) => write!(f, "SBVC Parse Error: {}", error),
            SbvcError::VersionNotFound(id) => {
                write!(f, "SBVC Error: Version with ID {} not nound", id)
            }
        }
    }
}

impl Error for SbvcError {}

/// The [`Result`] type for this crate.
///
/// [`Result`]: Result
pub type SbvcResult<T> = Result<T, SbvcError>;

/// A struct that represents the file where the version tree is contained.
#[derive(Debug, Clone)]
pub struct Sbvc {
    path: PathBuf,
    file: PathBuf,
    current: usize,
    next: u32,
    versions: Vec<Version>,
}

impl Sbvc {
    /// Creates a new [`Sbvc`] instance and creates the version tree file.
    ///
    /// # Errors
    ///
    /// This method will return an error if an IO error occurs.
    ///
    /// [`Sbvc`]: Sbvc
    pub fn new(path: PathBuf, file: PathBuf) -> SbvcResult<Self> {
        let sbvc = Sbvc {
            path,
            file,
            current: 0,
            next: 1,
            versions: vec![Version {
                id: 0,
                base: 0,
                name: INIT_VERSION_NAME.to_string(),
                date: SystemTime::now(),
                difference: OwnedDifference::empty(),
            }],
        };
        sbvc.write()?;
        Ok(sbvc)
    }

    /// Constructs a [`Sbvc`] instance from path to file containing version
    /// tree of a file.
    ///
    /// # Errors
    ///
    /// This method will return an error if an IO error or parsing error occurs.
    ///
    /// [`Sbvc`]: Sbvc
    pub fn open(path: PathBuf) -> SbvcResult<Self> {
        let source = fs::read(&path)?;
        let mut iter = NelfIter::from_string(&source);

        let file = OsStr::from_bytes(iter.next().ok_or_else(|| {
            SbvcError::InvalidFormat("Expected filename".to_string())
        })?)
        .into();

        let current_id = str::from_utf8(iter.next().ok_or_else(|| {
            SbvcError::InvalidFormat("Expected current version ID".to_string())
        })?)?
        .parse()?;

        let next = str::from_utf8(iter.next().ok_or_else(|| {
            SbvcError::InvalidFormat("Expected next version ID".to_string())
        })?)?
        .parse()?;

        let mut versions = Vec::new();

        for version in NelfIter::from_string(iter.next().ok_or_else(|| {
            SbvcError::InvalidFormat("Expected list of versions".to_string())
        })?)
        .map(|source| -> SbvcResult<Version> {
            let mut iter = NelfIter::from_string(source);

            let id = str::from_utf8(iter.next().ok_or_else(|| {
                SbvcError::InvalidFormat("Expected version id".to_string())
            })?)?
            .parse()?;

            let base = str::from_utf8(iter.next().ok_or_else(|| {
                SbvcError::InvalidFormat("Expected base version id".to_string())
            })?)?
            .parse()?;

            let mut meta =
                NelfIter::from_string(iter.next().ok_or_else(|| {
                    SbvcError::InvalidFormat(
                        "Expected version metadata".to_string(),
                    )
                })?);

            let name = str::from_utf8(meta.next().ok_or_else(|| {
                SbvcError::InvalidFormat("Expected version name".to_string())
            })?)?
            .to_string();

            let date = SystemTime::UNIX_EPOCH
                + Duration::from_secs(
                    str::from_utf8(meta.next().ok_or_else(|| {
                        SbvcError::InvalidFormat(
                            "Expected version creation date".to_string(),
                        )
                    })?)?
                    .parse()?,
                );

            let mut difference = OwnedDifference::empty();

            for deletion in
                NelfIter::from_string(iter.next().ok_or_else(|| {
                    SbvcError::InvalidFormat(
                        "Expected version deletions".to_string(),
                    )
                })?)
                .map(|source| -> SbvcResult<Deletion> {
                    let mut iter = NelfIter::from_string(source);

                    let start: usize =
                        str::from_utf8(iter.next().ok_or_else(|| {
                            SbvcError::InvalidFormat(
                                "Expected deletion start".to_string(),
                            )
                        })?)?
                        .parse()?;

                    let end: usize =
                        str::from_utf8(iter.next().ok_or_else(|| {
                            SbvcError::InvalidFormat(
                                "Expected deletion end".to_string(),
                            )
                        })?)?
                        .parse()?;

                    Ok(Deletion { start, end })
                })
            {
                difference.deletions.push(deletion?);
            }

            for insertion in
                NelfIter::from_string(iter.next().ok_or_else(|| {
                    SbvcError::InvalidFormat(
                        "Expected version insertions".to_string(),
                    )
                })?)
                .map(
                    |source| -> SbvcResult<OwnedInsertion<u8>> {
                        let mut iter = NelfIter::from_string(source);

                        let start: usize =
                            str::from_utf8(iter.next().ok_or_else(|| {
                                SbvcError::InvalidFormat(
                                    "Expected insertion start".to_string(),
                                )
                            })?)?
                            .parse()?;

                        let data = iter
                            .next()
                            .ok_or_else(|| {
                                SbvcError::InvalidFormat(
                                    "Expected insertion data".to_string(),
                                )
                            })?
                            .to_vec();

                        Ok(OwnedInsertion { start, data })
                    },
                )
            {
                difference.insertions.push(insertion?);
            }

            Ok(Version { id, base, name, date, difference })
        }) {
            versions.push(version?);
        }

        let current = versions
            .iter()
            .enumerate()
            .find(|&(_, version)| version.id == current_id)
            .map(|(index, _)| index)
            .ok_or(SbvcError::VersionNotFound(current_id))?;

        Ok(Sbvc { path, file, current, next, versions })
    }

    fn write(&self) -> SbvcResult<()> {
        fs::write(
            &self.path,
            [
                self.file.as_os_str().as_bytes(),
                self.versions[self.current].id.to_string().as_bytes(),
                self.next.to_string().as_bytes(),
                &self
                    .versions
                    .iter()
                    .map(|version| {
                        [
                            version.id.to_string().as_bytes(),
                            version.base.to_string().as_bytes(),
                            &[
                                version.name.as_bytes(),
                                version
                                    .date
                                    .duration_since(SystemTime::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs()
                                    .to_string()
                                    .as_bytes(),
                            ]
                            .to_newline_nelf(),
                            &version
                                .difference
                                .deletions
                                .iter()
                                .map(|deletion| {
                                    [
                                        deletion.start.to_string().as_bytes(),
                                        deletion.end.to_string().as_bytes(),
                                    ]
                                    .to_newline_nelf()
                                })
                                .to_newline_nelf(),
                            &version
                                .difference
                                .insertions
                                .iter()
                                .map(|insertion| {
                                    [
                                        insertion.start.to_string().as_bytes(),
                                        &insertion.data,
                                    ]
                                    .to_newline_nelf()
                                })
                                .to_newline_nelf(),
                        ]
                        .to_newline_nelf()
                    })
                    .to_newline_nelf(),
            ]
            .to_newline_nelf(),
        )?;

        Ok(())
    }

    fn data(&self, version: &Version) -> Vec<u8> {
        if version.id != version.base {
            let mut result =
                self.data(&self.versions[self.version(version.base).unwrap()]);
            result.patch(version.difference());
            result
        } else {
            Vec::new()
        }
    }

    fn rollback(&self) -> SbvcResult<()> {
        fs::write(&self.file, self.data(&self.versions[self.current]))?;
        Ok(())
    }

    /// Returns `true` if the traced file contents are not the same as the
    /// content for the current version.
    pub fn is_changed(&self) -> SbvcResult<bool> {
        Ok(fs::read(&self.file)? == self.data(&self.versions[self.current]))
    }

    /// Switches to the specified version using its ID.
    ///
    /// `rollback` specifies whether the contents of the file should be changed
    /// according to the version you are switching to. If `true`, file contents
    /// will be changed.
    ///
    /// # Errors
    ///
    /// Returns an error if an IO error happens or the supplied `id` is not
    /// found in the version tree. If `rollback` is `false` never fails.
    pub fn checkout(&mut self, id: u32, rollback: bool) -> SbvcResult<()> {
        self.current =
            self.version(id).ok_or(SbvcError::VersionNotFound(id))?;

        if rollback {
            self.rollback()?;
        }

        Ok(())
    }

    /// Saves changes in the file to a new version branching from the current
    /// one.
    ///
    /// This method automatically checks out the newly created version.
    ///
    /// # Errors
    ///
    /// This method fails if an IO error occurs.
    pub fn commit(&mut self) -> SbvcResult<()> {
        let content = fs::read(&self.file)?;

        self.versions.push(Version {
            id: self.next,
            base: self.versions[self.current].id,
            name: DEFAULT_VERSION_NAME.to_string(),
            date: SystemTime::now(),
            // TODO Optimize for big files
            difference: content
                .diff(&self.data(&self.versions[self.current]))
                .to_owned(),
        });
        self.next += 1;
        self.current = self.versions.len() - 1;
        self.write()
    }

    /// Renames the current version.
    ///
    /// # Errors
    ///
    /// This method returns an error when an IO error occurs.
    pub fn rename(&mut self, name: &str) -> SbvcResult<()> {
        self.versions[self.current].name.clear();
        self.versions[self.current].name.push_str(name);
        self.write()
    }

    /// Deletes version with the selected ID.
    ///
    /// This method does not delete the initial version.
    ///
    /// # Errors
    ///
    /// This method returns an error when and IO error occurs.
    pub fn delete(&mut self) -> SbvcResult<()> {
        let current = self.current;
        self.checkout(self.versions[self.current].base, true)?;
        self.delete_private(current);
        self.write()
    }

    fn delete_private(&mut self, index: usize) {
        let id = self.versions[index].id;

        if id != self.versions[index].base {
            while let Some(index) = self
                .versions
                .iter()
                .enumerate()
                .find(|&(_, version)| version.base == id)
                .map(|(index, _)| index)
            {
                self.delete_private(index);
            }

            self.versions.remove(self.version(id).unwrap());
        }
    }

    /// Returns the path to the version tree file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the path to the tracked file.
    pub fn file(&self) -> &Path {
        &self.file
    }

    /// Sets the tracked file for this version tree.
    ///
    /// # Errors
    ///
    /// Fails if an IO error occurs.
    pub fn set_file(&mut self, file: PathBuf) -> SbvcResult<()> {
        self.file = file;
        self.write()
    }

    /// Returns a reference to the current version (For info).
    pub fn current(&self) -> &Version {
        &self.versions[self.current]
    }

    fn version(&self, id: u32) -> Option<usize> {
        self.versions
            .iter()
            .enumerate()
            .find(|&(_, version)| version.id == id)
            .map(|(index, _)| index)
    }

    /// Returns a slice of all versions.
    pub fn versions(&self) -> &[Version] {
        &self.versions
    }
}

trait ToNewlineNelf {
    fn to_newline_nelf(self) -> Vec<u8>;
}

impl<T: IntoIterator<Item = V>, V: ToCell> ToNewlineNelf for T {
    fn to_newline_nelf(self) -> Vec<u8> {
        self.into_iter()
            .flat_map(|string| {
                let mut cell = string.to_cell();
                cell.push(b'\n');
                cell
            })
            .collect()
    }
}

/// An immutable representation of a version
#[derive(Debug, Clone)]
pub struct Version {
    id: u32,
    base: u32,
    name: String,
    date: SystemTime,
    difference: OwnedDifference<u8>,
}

impl Version {
    /// Returns the version ID.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Returns the base version ID.
    pub fn base(&self) -> u32 {
        self.base
    }

    /// Returns the version name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the version creation time.
    pub fn date(&self) -> SystemTime {
        self.date
    }

    /// Returns the difference of this version from the base version.
    pub fn difference(&self) -> Difference<u8> {
        self.difference.borrow()
    }
}
