//! # Single Binary file Version Control system
//!
//! This crate is backend for SBVC that provides useful and simple API to use
//! in the frontend.
//!
//! # Get Started
//!
//! For this crate to work you have to choose one (and *only* one) of the
//! following features:
//!
//! * `runtime-actix-native-tls`
//! * `runtime-async-std-native-tls`
//! * `runtime-tokio-native-tls`
//! * `runtime-actix-rustls`
//! * `runtime-async-std-rusttls`
//! * `runtime-tokio-rustls`
//!
//! By default, `runtime-async-std-rustls` is chosen. If you want to use other
//! runtime, disable default features of the crate.
//!
//! ## Example
//!
//! ```toml
//! [dependencies]
//! sbvc_lib = {
//!     version = "0.3.1",
//!     default-features = false,
//!     features = "runtime-tokio-native-tls",
//! }
//! ```
//!
//! To get starting using the API, refer to documentations of following
//! `struct`s:
//!
//! * [`Database`]
//! * [`Version`]
//!
//! [`Database`]: Database
//! [`Version`]: Version

use std::{
    collections::HashMap, fs::File,
    io::{self, Read, Write},
    path::PathBuf, pin::Pin,
    sync::{Arc, Weak},
};

use futures::{join, prelude::*};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use wgdiff::{Deletion, Diff, OwnedDifference, OwnedInsertion, Patch};

#[cfg(feature = "async-std")]
use async_std::sync::RwLock;
#[cfg(feature = "tokio")]
use tokio::sync::RwLock;

const INIT_VERSION_NAME: &'static str = "init";
const DEFAULT_VERSION_NAME: &'static str = "unnamed";

struct DatabaseInfo {
    path: PathBuf,
    pool: SqlitePool,
    versions: Version,
}

/// A `struct` that represents the database file where the version tree is
/// contained.
#[derive(Clone)]
pub struct Database(Arc<DatabaseInfo>);

impl Database {
    /// Constructs new [`Database`] from path to file version of which has to be
    /// controlled.
    ///
    /// This method creates `{path}.db` database file if it does not exist.
    ///
    /// To close the database, use [`close`] method.
    ///
    /// # Errors
    ///
    /// This method will return an error if anything goes wrong while connecting
    /// to the database (Including creating a file and wrongly designed database
    /// file already existing with the provided name)
    ///
    /// [`Database`]: Database
    /// [`close`]: Database::close
    pub async fn new(path: PathBuf) -> sqlx::Result<Self> {
        let mut path = path.into_os_string();
        path.push(".db");
        let mut path: PathBuf = path.into();

        let pool = SqlitePool::connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true)
        ).await?;

        sqlx::query("CREATE TABLE IF NOT EXISTS versions(
            id INTEGER PRIMARY KEY NOT NULL,
            bid INTEGER,
            name TEXT NOT NULL,
            date TEXT NOT NULL,
            FOREIGN KEY(bid) REFERENCES versions(id)
        )").execute(&pool).await?;

        sqlx::query("CREATE TABLE IF NOT EXISTS deletions(
            id INTEGER NOT NULL,
            start INTEGER NOT NULL,
            end INTEGER NOT NULL,
            PRIMARY KEY(id, start),
            FOREIGN KEY(id) REFERENCES versions(id)
        )").execute(&pool).await?;

        sqlx::query("CREATE TABLE IF NOT EXISTS insertions(
            id INTEGER NOT NULL,
            start INTEGER NOT NULL,
            data BLOB NOT NULL,
            PRIMARY KEY(id, start),
            FOREIGN KEY(id) REFERENCES versions(id)
        )").execute(&pool).await?;

        if sqlx::query_scalar::<_, u32>("SELECT COUNT() FROM versions")
            .fetch_one(&pool)
            .await? == 0
        {
            sqlx::query("INSERT INTO versions(name, date) VALUES(
                ?,
                datetime(\"now\", \"localtime\")
            )").bind(INIT_VERSION_NAME)
                .execute(&pool)
                .await?;
        }

        let versions = Version::new(&pool).await?;

        path.set_extension("");

        let database = Database(Arc::new(DatabaseInfo {
            path,
            pool,
            versions,
        }));

        database.versions().set_database(&database).await;

        Ok(database)
    }

    /// Creates a new weak pointer to `self`.
    pub fn downgrade(&self) -> DatabaseWeak {
        DatabaseWeak(Arc::downgrade(&self.0))
    }

    /// Closes all the connections to the database in the pool.
    pub async fn close(self) {
        self.0.pool.close().await;
    }

    /// Returns path to file version of which is being controlled.
    pub fn path(&self) -> PathBuf {
        self.0.path.clone()
    }

    /// Returns the root [`Version`].
    ///
    /// [`Version`]: Version
    pub fn versions(&self) -> Version {
        self.0.versions.clone()
    }
}

/// A weak pointer to [`Database`].
///
/// [`Database`]: Database
#[derive(Clone)]
pub struct DatabaseWeak(Weak<DatabaseInfo>);

impl DatabaseWeak {
    /// A convinience method that constructs a [`DatabaseWeak`] that does not
    /// point to anything.
    ///
    /// Calling [`upgrade`] method on the returned value will result in
    /// [`None`].
    ///
    /// [`DatabaseWeak`]: DatabaseWeak
    /// [`upgrade`]: DatabaseWeak::upgrade
    /// [`None`]: None
    pub fn new() -> DatabaseWeak {
        DatabaseWeak(Weak::new())
    }

    /// Attempts to upgrade value in `self` into a [`Database`].
    ///
    /// [`Database`]: Database
    pub fn upgrade(&self) -> Option<Database> {
        self.0.upgrade().map(|info| Database(info))
    }
}

struct VersionInfo {
    id: u32,
    base: VersionWeak,
    name: String,
    date: String,
    difference: OwnedDifference<u8>,
    children: Vec<Version>,
    database: DatabaseWeak,
}

/// A `struct` that represents a single commit in the version tree. It uses
/// [`Arc`] under the hood, so it is okay to [`clone`] it, as it has very little
/// cost.
///
/// [`Arc`]: Arc
/// [`clone`]: Clone::clone
#[derive(Clone)]
pub struct Version(Arc<RwLock<VersionInfo>>);

impl Version {
    async fn new(pool: &SqlitePool) -> sqlx::Result<Self> {
        let stream =
            sqlx::query_as("SELECT id, bid, name, date FROM versions")
                .fetch(pool)
                .try_filter_map(|(id, bid, name, date)| Box::pin(async move {
                    let deletions =
                        sqlx::query_as("
                            SELECT start, end FROM deletions
                            WHERE id = ? ORDER BY start
                        ")
                        .bind(id)
                        .fetch(pool)
                        .try_filter_map(|(start, end): (u32, u32)| async move {
                            Ok(Some(start as usize..end as usize))
                        })
                        .try_collect();

                    let insertions =
                        sqlx::query_as("
                            SELECT start, data FROM insertions
                            WHERE id = ? ORDER by start
                        ")
                        .bind(id)
                        .fetch(pool)
                        .try_filter_map(|(start, data): (u32, _)| async move {
                            Ok(Some(OwnedInsertion::new(start as usize, data)))
                        })
                        .try_collect();

                    let (deletions, insertions) = join!(deletions, insertions);
                    let deletions = deletions?;
                    let insertions = insertions?;

                    Ok(Some((
                        VersionInfo {
                            id,
                            base: VersionWeak::new(),
                            name,
                            date,
                            difference:
                                OwnedDifference::new(deletions, insertions),
                            children: Vec::new(),
                            database: DatabaseWeak::new(),
                        },
                        bid
                    )))
                }));

        let mut map = HashMap::new();

        stream.try_for_each_concurrent(None, |(info, bid)| {
            map.insert(info.id, (Version(Arc::new(RwLock::new(info))), bid));
            future::ready(Ok(()))
        }).await?;

        let mut root = VersionWeak::new();

        for (version, bid) in map.values() {
            if let Some(bid) = bid {
                let parent = async {
                    map[bid].0.0.write().await.children.push(version.clone());
                };
                let child = async {
                    version.0.write().await.base = map[bid].0.downgrade();
                };
                join!(parent, child);
            } else {
                root = version.downgrade();
            }
        }

        Ok(root.upgrade().unwrap())
    }

    fn set_database<'a>(
        &'a self,
        database: &'a Database,
    ) -> Pin<Box<dyn 'a + Send + Sync + Future<Output = ()>>> {
        Box::pin(async move {
            self.0.write().await.database = database.downgrade();

            for child in &self.0.read().await.children {
                child.set_database(database).await;
            }
        })
    }

    /// Returns contents of the controlled file in given version.
    ///
    /// This method does not cache anything or use any cached values. Instead,
    /// it calculates contents of the file every time this method is called
    /// using differences contained in the database, so you should either cache
    /// contents yourself, or do not call this method very often.
    pub fn data(
        &self,
    ) -> Pin<Box<dyn '_ + Send + Sync + Future<Output = Vec<u8>>>> {
        Box::pin(async {
            let read = self.0.read().await;
            if let Some(base) = read.base.upgrade() {
                let mut vec = base.data().await;
                vec.patch(read.difference.borrow());
                vec
            } else {
                Vec::new()
            }
        })
    }

    /// Deletes given version and all its children from the version tree.
    ///
    /// # Errors
    ///
    /// This method returns an error if error happens while deleting records
    /// from the database.
    pub async fn delete(&self) -> sqlx::Result<()> {
        let parent = async {
            let read = self.0.read().await;

            if let Some(base) = read.base.upgrade() {
                let children = &mut base.0.write().await.children;
                let mut found = None;
                for (index, version) in children.iter().enumerate() {
                    if version.0.read().await.id == read.id {
                        found = Some(index);
                        break;
                    }
                }
                children.remove(found.unwrap());
            }
        };

        let children = self.delete_private();

        let (_, children) = join!(parent, children);
        children?;

        Ok(())
    }

    fn delete_private(
        &self,
    ) -> Pin<Box<dyn '_ + Send + Future<Output = sqlx::Result<()>>>> {
        Box::pin(async {
            let read = self.0.read().await;

            let children = async {
                for child in &read.children {
                    child.delete_private().await?;
                }

                Ok(())
            };

            let database = read.database.upgrade().unwrap();

            let deletions = sqlx::query("DELETE FROM deletions WHERE id = ?")
                .bind(read.id)
                .execute(&database.0.pool);
            let insertions = sqlx::query("DELETE FROM insertions WHERE id = ?")
                .bind(read.id)
                .execute(&database.0.pool);

            let (children, deletions, insertions): (sqlx::Result<()>, _, _) =
                join!(children, deletions, insertions);
            children?;
            deletions?;
            insertions?;

            sqlx::query("DELETE FROM versions WHERE id = ?")
                .bind(read.id)
                .execute(&database.0.pool)
                .await?;

            Ok(())
        })
    }

    /// Rolls the controlled file back to its state at given version.
    ///
    /// # Errors
    ///
    /// This method returns an error if an IO error occurs while trying to write
    /// into the file.
    pub async fn rollback(&self) -> io::Result<()> {
        let read = self.0.read();
        let data = self.data();

        let (read, data) = join!(read, data);

        File::create(&read.database.upgrade().unwrap().0.path)?.write(&data)?;
        Ok(())
    }

    /// Renames the version to given name.
    ///
    /// # Errors
    ///
    /// This method returns an error if an error occurs while trying to update
    /// values in the database.
    pub async fn rename(&self, name: String) -> sqlx::Result<()> {
        let read = self.0.read().await;

        sqlx::query("UPDATE versions SET name = ? WHERE id = ?")
            .bind(&name)
            .bind(read.id)
            .execute(&read.database.upgrade().unwrap().0.pool)
            .await?;

        drop(read);

        self.0.write().await.name = name;

        Ok(())
    }

    /// Commits file's new state into the version tree.
    ///
    /// The new version will be child of `self`.
    ///
    /// # Errors
    ///
    /// This method returns an error if an error occurs while trying to insert
    /// values into the database.
    pub async fn commit(&self) -> sqlx::Result<()> {
        let read = self.0.read().await;

        let database = read.database.upgrade().unwrap();

        let query = sqlx::query_as("
            INSERT INTO versions(bid, name, date)
            VALUES(?, ?, datetime(\"now\", \"localtime\"))
            RETURNING id, name, date
        ").bind(read.id)
            .bind(DEFAULT_VERSION_NAME)
            .fetch_one(&database.0.pool);

        let old = self.data();

        let (query, old) = join!(query, old);
        let (id, name, date) = query?;

        let mut new = Vec::new();
        File::open(&read.database.upgrade().unwrap().0.path)?
            .read_to_end(&mut new)?;

        let chunk_size = (old.len().min(new.len()) / 1_000).max(1);

        let old = old.chunks(chunk_size).collect();
        let new: Vec<_> = new.chunks(chunk_size).collect();

        let mut difference = new.diff(&old);

        for Deletion { start, end } in &mut difference.deletions {
            let new_start = *start * chunk_size;
            *end = new_start
                + old[*start..*end].iter()
                    .fold(0, |result, chunk| result + chunk.len());
            *start = new_start;
        }

        let difference = OwnedDifference::new(
            difference.deletions,
            difference.insertions.into_iter()
                .map(|insertion| {
                    OwnedInsertion::new(
                        insertion.start * chunk_size,
                        insertion.data.into_iter()
                            .map(|slice| slice.iter())
                            .flatten()
                            .map(Clone::clone)
                            .collect(),
                    )
                })
                .collect(),
        );

        let deletions = async {
            for Deletion { start, end } in &difference.deletions {
                sqlx::query(
                    "INSERT INTO deletions(id, start, end) VALUES(?, ?, ?)",
                ).bind(id)
                    .bind(*start as u32)
                    .bind(*end as u32)
                    .execute(&database.0.pool)
                    .await?;
            }
            Ok(())
        };

        let insertions = async {
            for OwnedInsertion { start, data } in &difference.insertions {
                sqlx::query(
                    "INSERT INTO insertions(id, start, data) VALUES(?, ?, ?)",
                ).bind(id)
                    .bind(*start as u32)
                    .bind(data)
                    .execute(&database.0.pool)
                    .await?;
            }
            Ok(())
        };

        let (deletions, insertions): (sqlx::Result<()>, sqlx::Result<()>) =
            join!(deletions, insertions);
        deletions?;
        insertions?;

        let info = VersionInfo {
            id,
            base: self.downgrade(),
            name,
            date,
            difference,
            children: Vec::new(),
            database: database.downgrade(),
        };

        drop(read);

        self.0.write().await.children.push(
            Version(Arc::new(RwLock::new(info))),
        );

        Ok(())
    }

    /// Creates a new weak pointer to `self`.
    pub fn downgrade(&self) -> VersionWeak {
        VersionWeak(Arc::downgrade(&self.0))
    }

    /// Returns ID of the version.
    pub async fn id(&self) -> u32 {
        self.0.read().await.id
    }

    /// Returns parent version of `self`. Returns [`None`] if there is none.
    ///
    /// [`None`]: None
    pub async fn base(&self) -> Option<Version> {
        self.0.read().await.base.upgrade()
    }

    /// Returns name of the version.
    pub async fn name(&self) -> String {
        self.0.read().await.name.clone()
    }

    /// Returns date when the version was commited.
    ///
    /// This method returns [`String`] representation of the date in the same
    /// format it was stores in the database.
    ///
    /// [`String`]: String
    pub async fn date(&self) -> String {
        self.0.read().await.date.clone()
    }

    /// Returns number of deletions from the file in this version.
    pub async fn deletions(&self) -> usize {
        self.0.read().await.difference.deletions.len()
    }

    /// Returns number of insertions into the file in this version.
    pub async fn insertions(&self) -> usize {
        self.0.read().await.difference.insertions.len()
    }

    /// Returns all the children of the version.
    pub async fn children(&self) -> Vec<Version> {
        self.0.read().await.children.clone()
    }
}

/// A weak pointer to [`Version`].
///
/// [`Version`]: Version
#[derive(Clone)]
pub struct VersionWeak(Weak<RwLock<VersionInfo>>);

impl VersionWeak {
    /// A convinience method that constructs a [`VersionWeak`] that does not
    /// point to anything.
    ///
    /// Calling [`upgrade`] method on the returned value will result in
    /// [`None`].
    ///
    /// [`VersionWeak`]: VersionWeak
    /// [`upgrade`]: VersionWeak::upgrade
    /// [`None`]: None
    pub fn new() -> VersionWeak {
        VersionWeak(Weak::new())
    }

    /// Attempts to upgrade value in `self` into a [`Version`].
    ///
    /// [`Version`]: Version
    pub fn upgrade(&self) -> Option<Version> {
        self.0.upgrade().map(|info| Version(info))
    }
}
