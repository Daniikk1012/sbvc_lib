use std::fs;

use sbvc_lib::Database;

#[cfg_attr(feature = "actix-rt", actix_rt::test)]
#[cfg_attr(feature = "async-std", async_std::test)]
#[cfg_attr(feature = "tokio", tokio::test)]
async fn create() -> sqlx::Result<()> {
    const FILE: &'static str = "creation";
    const DATABASE: &'static str = "creation.db";

    Database::new(FILE.into()).await?.close().await;

    fs::remove_file(DATABASE)?;
    
    Ok(())
}

#[cfg_attr(feature = "actix-rt", actix_rt::test)]
#[cfg_attr(feature = "async-std", async_std::test)]
#[cfg_attr(feature = "tokio", tokio::test)]
async fn data() -> sqlx::Result<()> {
    const FILE: &'static str = "data";
    const DATABASE: &'static str = "data.db";
    const DATA_1: &'static [u8] = b"SOME DATA TO PUT INTO FILE";
    const DATA_2: &'static [u8] = b"SOME OTHER DATA TO REPLACE WHAT WAS BEFORE";

    let database = Database::new(FILE.into()).await?;
    fs::write(FILE, DATA_1)?;
    database.versions().commit().await?;
    assert_eq!(database.versions().children().await[0].data().await, DATA_1);
    database.close().await;

    let database = Database::new(FILE.into()).await?;
    fs::write(FILE, DATA_2)?;
    database.versions().children().await[0].commit().await?;
    assert_eq!(database.versions().children().await[0].data().await, DATA_1);
    assert_eq!(
        database.versions()
            .children()
            .await[0]
            .children()
            .await[0]
            .data()
            .await,
        DATA_2,
    );
    database.close().await;

    let database = Database::new(FILE.into()).await?;
    assert_eq!(database.versions().children().await[0].data().await, DATA_1);
    assert_eq!(
        database.versions()
            .children()
            .await[0]
            .children()
            .await[0]
            .data()
            .await,
        DATA_2,
    );
    database.close().await;

    fs::remove_file(FILE)?;
    fs::remove_file(DATABASE)?;

    Ok(())
}

#[cfg_attr(feature = "actix-rt", actix_rt::test)]
#[cfg_attr(feature = "async-std", async_std::test)]
#[cfg_attr(feature = "tokio", tokio::test)]
async fn delete() -> sqlx::Result<()> {
    const FILE: &'static str = "delete";
    const DATABASE: &'static str = "delete.db";
    const DATA_1: &'static [u8] = b"SOME DATA TO PUT INTO FILE";
    const DATA_2: &'static [u8] = b"SOME OTHER DATA TO REPLACE WHAT WAS BEFORE";

    let database = Database::new(FILE.into()).await?;
    fs::write(FILE, DATA_1)?;
    database.versions().commit().await?;
    fs::write(FILE, DATA_2)?;
    database.versions().children().await[0].commit().await?;
    database.versions().children().await[0].delete().await?;
    database.close().await;

    let database = Database::new(FILE.into()).await?;
    assert!(database.versions().children().await.is_empty());
    database.close().await;

    fs::remove_file(FILE)?;
    fs::remove_file(DATABASE)?;

    Ok(())
}

#[cfg_attr(feature = "actix-rt", actix_rt::test)]
#[cfg_attr(feature = "async-std", async_std::test)]
#[cfg_attr(feature = "tokio", tokio::test)]
async fn rollback() -> sqlx::Result<()> {
    const FILE: &'static str = "rollback";
    const DATABASE: &'static str = "rollback.db";
    const DATA_1: &'static [u8] = b"SOME DATA TO PUT INTO FILE";
    const DATA_2: &'static [u8] = b"SOME OTHER DATA TO REPLACE WHAT WAS BEFORE";

    let database = Database::new(FILE.into()).await?;
    fs::write(FILE, DATA_1)?;
    database.versions().commit().await?;
    fs::write(FILE, DATA_2)?;
    database.versions().children().await[0].commit().await?;
    database.versions().children().await[0].rollback().await?;
    assert_eq!(fs::read(FILE)?, DATA_1);
    database.close().await;

    fs::remove_file(FILE)?;
    fs::remove_file(DATABASE)?;

    Ok(())
}

#[cfg_attr(feature = "actix-rt", actix_rt::test)]
#[cfg_attr(feature = "async-std", async_std::test)]
#[cfg_attr(feature = "tokio", tokio::test)]
async fn rename() -> sqlx::Result<()> {
    const FILE: &'static str = "rename";
    const DATABASE: &'static str = "rename.db";
    const NAME: &'static str = "new name";

    let database = Database::new(FILE.into()).await?;
    database.versions().rename(NAME.to_string()).await?;
    assert_eq!(database.versions().name().await, NAME);
    database.close().await;

    let database = Database::new(FILE.into()).await?;
    assert_eq!(database.versions().name().await, NAME);
    database.close().await;

    fs::remove_file(DATABASE)?;

    Ok(())
}
