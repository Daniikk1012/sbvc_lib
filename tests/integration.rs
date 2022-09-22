use std::{fs, str};

use sbvc_lib::{Sbvc, SbvcResult};

#[test]
fn create() -> SbvcResult<()> {
    const PATH: &str = "creation.nelf";
    const FILE: &str = "creation";

    Sbvc::new(PATH.into(), FILE.into())?;
    fs::remove_file(PATH)?;
    Ok(())
}

#[test]
fn rollback() -> SbvcResult<()> {
    const PATH: &str = "rollback.nelf";
    const FILE: &str = "rollback";
    const DATA_1: &[u8] = b"SOME DATA TO PUT INTO FILE";
    const DATA_2: &[u8] = b"SOME OTHER DATA TO REPLACE WHAT WAS BEFORE";

    let _ = fs::remove_file(FILE);

    let mut sbvc = Sbvc::new(PATH.into(), FILE.into())?;
    assert!(sbvc.is_changed().is_err());
    fs::write(FILE, DATA_1)?;
    assert!(sbvc.is_changed()?);
    sbvc.commit()?;
    assert!(!sbvc.is_changed()?);
    fs::write(FILE, DATA_2)?;
    sbvc.commit()?;

    let mut sbvc = Sbvc::open(PATH.into())?;
    sbvc.checkout(1, true)?;
    assert_eq!(fs::read(FILE)?, DATA_1);
    sbvc.checkout(2, true)?;
    assert_eq!(fs::read(FILE)?, DATA_2);

    fs::remove_file(PATH)?;
    fs::remove_file(FILE)?;

    Ok(())
}

#[test]
fn delete() -> SbvcResult<()> {
    const PATH: &str = "delete.nelf";
    const FILE: &str = "delete";
    const DATA_1: &[u8] = b"SOME DATA TO PUT INTO FILE";
    const DATA_2: &[u8] = b"SOME OTHER DATA TO REPLACE WHAT WAS BEFORE";

    let mut sbvc = Sbvc::new(PATH.into(), FILE.into())?;
    fs::write(FILE, DATA_1)?;
    sbvc.commit()?;
    fs::write(FILE, DATA_2)?;
    sbvc.commit()?;
    sbvc.commit()?;
    sbvc.checkout(2, true)?;
    sbvc.delete()?;

    let sbvc = Sbvc::open(PATH.into())?;
    assert_eq!(
        sbvc.versions().iter().map(|version| version.id()).collect::<Vec<_>>(),
        [0, 1]
    );

    fs::remove_file(PATH)?;
    fs::remove_file(FILE)?;

    Ok(())
}

#[test]
fn rename() -> SbvcResult<()> {
    const PATH: &str = "rename.nelf";
    const FILE: &str = "rename";
    const NAME: &str = "new name";

    let mut sbvc = Sbvc::new(PATH.into(), FILE.into())?;
    sbvc.rename(NAME)?;

    let sbvc = Sbvc::open(PATH.into())?;
    assert_eq!(sbvc.current().name(), NAME);

    fs::remove_file(PATH)?;

    Ok(())
}
