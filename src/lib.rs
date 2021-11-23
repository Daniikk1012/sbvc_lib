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
//!     version = "0.1",
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

use futures::prelude::*;
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
            id INTEGER PRIMARY KEY,
            bid INTEGER,
            name TEXT,
            date TIMESTAMP,
            FOREIGN KEY(bid) REFERENCES versions(id)
        )").execute(&pool).await?;

        sqlx::query("CREATE TABLE IF NOT EXISTS deletions(
            id INTEGER,
            start INTEGER,
            end INTEGER,
            PRIMARY KEY(id, start),
            FOREIGN KEY(id) REFERENCES versions(id)
        )").execute(&pool).await?;

        sqlx::query("CREATE TABLE IF NOT EXISTS insertions(
            id INTEGER,
            start INTEGER,
            data BLOB,
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

struct VersionInfo {
    id: u32,
    base: Weak<RwLock<VersionInfo>>,
    name: String,
    date: String,
    difference: OwnedDifference<u8>,
    children: Vec<Version>,
    database: Weak<DatabaseInfo>,
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
                        .try_collect()
                        .await?;

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
                        .try_collect()
                        .await?;

                    Ok(Some((
                        VersionInfo {
                            id,
                            base: Weak::new(),
                            name,
                            date,
                            difference:
                                OwnedDifference::new(deletions, insertions),
                            children: Vec::new(),
                            database: Weak::new(),
                        },
                        bid
                    )))
                }));

        let mut map = HashMap::new();

        stream.try_for_each(|(info, bid)| {
            map.insert(info.id, (Arc::new(RwLock::new(info)), bid));
            future::ready(Ok(()))
        }).await?;

        let mut root = None;

        for (info, bid) in map.values() {
            if let Some(bid) = bid {
                map[bid].0.write().await.children.push(Version(info.clone()));
                info.write().await.base = Arc::downgrade(&map[bid].0);
            } else {
                root = Some(Version(info.clone()));
            }
        }

        Ok(root.unwrap())
    }

    fn set_database<'a>(&'a self, database: &'a Database) -> Pin<Box<dyn 'a + Sync + Future<Output = ()>>> {
        Box::pin(async move {
            self.0.write().await.database = Arc::downgrade(&database.0);
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
    pub fn data(&self) -> Pin<Box<dyn '_ + Sync + Future<Output = Vec<u8>>>> {
        Box::pin(async {
            if let Some(base) = self.0.read().await.base.upgrade() {
                let mut vec = Version(base).data().await;
                vec.patch(self.0.read().await.difference.borrow());
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
        self.delete_private().await?;

        if let Some(base) = self.0.read().await.base.upgrade() {
            let children = &mut base.write().await.children;
            let mut found = None;
            for (index, version) in children.iter().enumerate() {
                if version.0.read().await.id == self.0.read().await.id {
                    found = Some(index);
                }
            }
            children.remove(found.unwrap());
        }

        Ok(())
    }

    fn delete_private(
        &self,
    ) -> Pin<Box<dyn '_ + Future<Output = sqlx::Result<()>>>> {
        Box::pin(async {
            for child in &self.0.read().await.children {
                child.delete_private().await?;
            }

            sqlx::query("DELETE FROM deletions WHERE id = ?")
                .bind(self.0.read().await.id)
                .execute(&self.0.read().await.database.upgrade().unwrap().pool)
                .await?;
            sqlx::query("DELETE FROM insertions WHERE id = ?")
                .bind(self.0.read().await.id)
                .execute(&self.0.read().await.database.upgrade().unwrap().pool)
                .await?;
            sqlx::query("DELETE FROM versions WHERE id = ?")
                .bind(self.0.read().await.id)
                .execute(&self.0.read().await.database.upgrade().unwrap().pool)
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
        File::create(&self.0.read().await.database.upgrade().unwrap().path)?
            .write(&self.data().await)?;
        Ok(())
    }

    /// Renames the version to given name.
    ///
    /// # Errors
    ///
    /// This method returns an error if an error occurs while trying to update
    /// values in the database.
    pub async fn rename(&self, name: String) -> sqlx::Result<()> {
        sqlx::query("UPDATE versions SET name = ? WHERE id = ?")
            .bind(&name)
            .bind(self.0.read().await.id)
            .execute(&self.0.read().await.database.upgrade().unwrap().pool)
            .await?;

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
        let (id, name, date) = sqlx::query_as("
            INSERT INTO versions(bid, name, date)
            VALUES(?, ?, datetime(\"now\", \"localtime\"))
            RETURNING id, name, date
        ").bind(self.0.read().await.id)
            .bind(DEFAULT_VERSION_NAME)
            .fetch_one(&self.0.read().await.database.upgrade().unwrap().pool)
            .await?;

        let old = self.data().await;
        let mut new = Vec::new();
        File::open(&self.0.read().await.database.upgrade().unwrap().path)?
            .read_to_end(&mut new)?;

        let mut sqrt = (old.len() as f32).sqrt() as usize;
        if sqrt == 0 {
            sqrt = 1;
        }

        let old = old.chunks(sqrt).collect();
        let new: Vec<_> = new.chunks(sqrt).collect();

        let mut difference = new.diff(&old);
        
        for Deletion { start, end } in &mut difference.deletions {
            let new_start = *start * sqrt;
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
                        insertion.start * sqrt,
                        insertion.data.into_iter()
                            .map(|slice| slice.iter())
                            .flatten()
                            .map(Clone::clone)
                            .collect(),
                    )
                })
                .collect(),
        );

        for Deletion { start, end } in &difference.deletions {
            sqlx::query(
                "INSERT INTO deletions(id, start, end) VALUES(?, ?, ?)",
            ).bind(id)
                .bind(*start as u32)
                .bind(*end as u32)
                .execute(&self.0.read().await.database.upgrade().unwrap().pool)
                .await?;
        }

        for OwnedInsertion { start, data } in &difference.insertions {
            sqlx::query(
                "INSERT INTO insertions(id, start, data) VALUES(?, ?, ?)",
            ).bind(id)
                .bind(*start as u32)
                .bind(data)
                .execute(&self.0.read().await.database.upgrade().unwrap().pool)
                .await?;
        }

        let info = VersionInfo {
            id,
            base: Arc::downgrade(&self.0),
            name,
            date,
            difference,
            children: Vec::new(),
            database: self.0.read().await.database.clone(),
        };

        self.0.write().await.children.push(
            Version(Arc::new(RwLock::new(info))),
        );

        Ok(())
    }

    /// Returns ID of the version.
    pub async fn id(&self) -> u32 {
        self.0.read().await.id
    }

    /// Returns parent version of `self`. Returns [`None`] if there is none.
    ///
    /// [`None`]: None
    pub async fn base(&self) -> Option<Version> {
        self.0.read().await.base.upgrade().map(|info| Version(info))
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
